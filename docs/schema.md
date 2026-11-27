# Schema

Four tables in the application's PostgreSQL database (14 or later). Create them with
`webhook-outbox-relay migrate`, or apply [schema/postgresql.sql](../schema/postgresql.sql)
with your own migration tool. Use one or the other: `migrate` records what it applied in
`webhook_schema_migrations` and does not know about SQL run by hand.

| Table | One row per | Written by |
|---|---|---|
| `webhook_endpoints` | URL a customer wants webhooks sent to | `endpoints` commands |
| `webhook_messages` | event, inserted in the application's transaction | the application |
| `webhook_deliveries` | message to one endpoint | the relay |
| `webhook_attempts` | HTTP request the relay made | the relay |

- `customer` is the application's own id for whoever owns the endpoint: an account, a
  merchant, a tenant. A message goes to the enabled endpoints of its customer that take
  its type (`event_types` NULL means every type).
- The secret is stored as Standard Webhooks writes it, `whsec_` and base64 of 32 random
  bytes. The relay signs with it, so it cannot be a hash. Keep the table out of reach of
  anyone who should not be able to sign as you.
- `previous_secret` and `previous_secret_until` hold the secret before the last
  `endpoints rotate` while it still signs; see [Rotating a secret](#rotating-a-secret).
- `webhook_endpoints.failing_since` is set by the relay on the first failed attempt
  after a success and cleared by the next success. `endpoints enable` clears it too.
- `webhook_messages.payload` is the exact body that is signed and sent. The application
  serializes it once; the relay does not parse or rewrite it.
- `webhook_deliveries.leased_until` is set while a relay sends the delivery and cleared
  when the attempt is recorded. The relay counts these rows to limit the requests in
  flight to one endpoint.

## Endpoints

```
$ webhook-outbox-relay endpoints add -customer acme -url https://acme.example/hooks -types order.paid
id ep_2bq7v3k4...
secret whsec_...
$ webhook-outbox-relay endpoints list -customer acme
$ webhook-outbox-relay endpoints secret ep_2bq7v3k4...
$ webhook-outbox-relay endpoints rotate ep_2bq7v3k4... -keep 3d
$ webhook-outbox-relay endpoints disable ep_2bq7v3k4... -reason "customer asked"
$ webhook-outbox-relay endpoints enable ep_2bq7v3k4...
```

The database is taken from `DATABASE_URL`. `list` never prints secrets; `secret` prints
one, to hand to the customer. The URL must be absolute http or https, without
credentials or a fragment.

### Rotating a secret

`endpoints rotate` gives the endpoint a new secret and prints it. For `-keep` (24 hours
by default) the relay signs every request with both: `webhook-signature` carries the
new signature and then the old one, as Standard Webhooks allows. The customer can
switch their receiver to the new secret at any point in that window, and no webhook is
rejected on the way. After the window only the new secret signs.

```
$ webhook-outbox-relay endpoints rotate ep_2bq7v3k4... -keep 3d
secret whsec_...
old secret signs until 2026-11-30T10:00:00Z
```

`list` shows `old secret until ...` while the window is open. If the secret leaked, use
`-keep 0`: the old one stops signing at once, and the receiver rejects requests until
it has the new secret. Rotating again within the window drops the oldest secret; only
the one before the last rotation keeps signing.
