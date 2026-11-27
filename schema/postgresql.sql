-- The tables of webhook-outbox for PostgreSQL 14 or later, generated from
-- relay/internal/schema/migrations by make schema. Apply this file with your own
-- migration tool, or run webhook-outbox-relay migrate, which applies the same SQL and
-- records the version. Use one or the other, not both.

-- 0001_init.sql

-- Endpoints: where a customer of the application wants its webhooks.
CREATE TABLE webhook_endpoints (
    id              text PRIMARY KEY,
    customer        text NOT NULL,
    url             text NOT NULL,
    -- whsec_ followed by base64 of the key, as Standard Webhooks writes it. The relay
    -- signs with it, so it is stored as is.
    secret          text NOT NULL,
    -- NULL: every type.
    event_types     text[],
    description     text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    disabled_at     timestamptz,
    disabled_reason text,
    -- The first failed attempt after the last successful one. The relay disables the
    -- endpoint when this is older than its limit (5 days by default).
    failing_since   timestamptz
);
CREATE INDEX webhook_endpoints_customer ON webhook_endpoints (customer);

-- Messages: one row per event, inserted by the application in its own transaction.
CREATE TABLE webhook_messages (
    -- Sent as webhook-id on every attempt.
    id            text PRIMARY KEY,
    customer      text NOT NULL,
    type          text NOT NULL,
    -- The exact JSON body that is signed and sent.
    payload       text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    -- Set by the relay when it has created the deliveries for this message.
    dispatched_at timestamptz
);
CREATE INDEX webhook_messages_undispatched ON webhook_messages (created_at) WHERE dispatched_at IS NULL;
-- For replay -since: one customer's messages from a point in time.
CREATE INDEX webhook_messages_customer ON webhook_messages (customer, created_at);

-- Deliveries: one message to one endpoint.
CREATE TABLE webhook_deliveries (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id      text NOT NULL REFERENCES webhook_messages (id) ON DELETE CASCADE,
    endpoint_id     text NOT NULL REFERENCES webhook_endpoints (id),
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'succeeded', 'failed')),
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (message_id, endpoint_id)
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX webhook_deliveries_endpoint ON webhook_deliveries (endpoint_id, created_at);

-- Attempts: every HTTP request the relay made.
CREATE TABLE webhook_attempts (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id bigint NOT NULL REFERENCES webhook_deliveries (id) ON DELETE CASCADE,
    started_at  timestamptz NOT NULL,
    duration_ms integer NOT NULL,
    -- NULL when no response came: timeout, refused connection, TLS error.
    status_code integer,
    error       text,
    -- The start of the response body, cut to a limit.
    response    text
);
CREATE INDEX webhook_attempts_delivery ON webhook_attempts (delivery_id);

-- 0002_endpoint_limit.sql

-- Set when a relay claims a delivery and cleared when it records the attempt. A
-- delivery with leased_until in the future is a request in flight; a relay that died
-- leaves a lease that runs out on its own.
ALTER TABLE webhook_deliveries ADD COLUMN leased_until timestamptz;

-- The oldest due delivery of one endpoint, and its next ones in order.
CREATE INDEX webhook_deliveries_endpoint_due ON webhook_deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';
-- Requests in flight per endpoint. Only claimed rows are in it.
CREATE INDEX webhook_deliveries_leased ON webhook_deliveries (endpoint_id) WHERE leased_until IS NOT NULL;

-- 0003_secret_rotation.sql

-- The secret before the last rotation, still used to sign until previous_secret_until,
-- so a receiver can switch to the new one at any point in that window.
ALTER TABLE webhook_endpoints ADD COLUMN previous_secret text;
ALTER TABLE webhook_endpoints ADD COLUMN previous_secret_until timestamptz;
ALTER TABLE webhook_endpoints ADD CONSTRAINT webhook_endpoints_previous_secret
    CHECK ((previous_secret IS NULL) = (previous_secret_until IS NULL));
