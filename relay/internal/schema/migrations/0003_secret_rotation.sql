-- The secret before the last rotation, still used to sign until previous_secret_until,
-- so a receiver can switch to the new one at any point in that window.
ALTER TABLE webhook_endpoints ADD COLUMN previous_secret text;
ALTER TABLE webhook_endpoints ADD COLUMN previous_secret_until timestamptz;
ALTER TABLE webhook_endpoints ADD CONSTRAINT webhook_endpoints_previous_secret
    CHECK ((previous_secret IS NULL) = (previous_secret_until IS NULL));
