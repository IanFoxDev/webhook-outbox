-- Set when a relay claims a delivery and cleared when it records the attempt. A
-- delivery with leased_until in the future is a request in flight; a relay that died
-- leaves a lease that runs out on its own.
ALTER TABLE webhook_deliveries ADD COLUMN leased_until timestamptz;

-- The oldest due delivery of one endpoint, and its next ones in order.
CREATE INDEX webhook_deliveries_endpoint_due ON webhook_deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';
-- Requests in flight per endpoint. Only claimed rows are in it.
CREATE INDEX webhook_deliveries_leased ON webhook_deliveries (endpoint_id) WHERE leased_until IS NOT NULL;
