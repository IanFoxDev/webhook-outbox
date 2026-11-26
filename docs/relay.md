# Relay

`webhook-outbox-relay run` delivers what the application queued. It needs
`DATABASE_URL` and nothing else; every other setting has a default.

| Variable | Default | Meaning |
|---|---|---|
| `RELAY_POLL_INTERVAL` | `1s` | wait after a round that found less than a full batch |
| `RELAY_BATCH_SIZE` | `100` | messages dispatched and deliveries claimed per round |
| `RELAY_CONCURRENCY` | `16` | requests in flight at once |
| `RELAY_ENDPOINT_CONCURRENCY` | `4` | requests in flight to one endpoint, over all relays on the database; `0` for no limit |
| `RELAY_TIMEOUT` | `15s` | limit for one request, from connecting to reading the response |
| `RELAY_RESPONSE_LIMIT` | `4096` | bytes of a response body kept in `webhook_attempts` |
| `RELAY_RETRY_SCHEDULE` | `5s,5m,30m,2h,5h,10h,10h` | pause after each failed attempt; `none` for one attempt |
| `RELAY_DISABLE_AFTER` | `5d` | disable an endpoint when no attempt has succeeded for this long (`36h`, `7d`); `never` to keep it |
| `RELAY_ALLOW_PRIVATE_NETWORKS` | `false` | `true` lets the relay send to loopback, private and link-local addresses |
| `RELAY_HTTP_ADDR` | `:8080` | where `/metrics`, `/healthz` and `/readyz` are served |

`/healthz` answers 200 while the process runs; `/readyz` answers 200 when the database
answers a ping, 503 otherwise. The image runs `webhook-outbox-relay healthcheck` as its
Docker healthcheck. A relay whose port is taken exits with code 1 at start.

Logs are JSON on stderr. SIGTERM finishes the requests in flight, records them and
exits, so a stop takes up to `RELAY_TIMEOUT`.

## What a round does

1. **Dispatch.** Up to a batch of new messages (`dispatched_at IS NULL`) is locked with
   `FOR UPDATE SKIP LOCKED`. For each, one delivery is created per enabled endpoint of
   the message's customer that takes its type, and the message is marked dispatched.
   One statement, one transaction.
2. **Claim.** Up to a batch of pending deliveries that are due and whose endpoint is
   enabled is locked the same way, and their `next_attempt_at` is moved past the end of
   the request (timeout plus 30 seconds). The transaction commits before any request is
   made, so no lock is held while waiting for a customer's server.
3. **Send.** A POST with the payload as the body, `Content-Type: application/json` and
   the three Standard Webhooks headers. `webhook-id` is the message id.
4. **Record.** The attempt is inserted and the delivery moved on in one transaction.

## Outcomes

| Response | Delivery |
|---|---|
| 2xx | `succeeded` |
| anything else, a timeout, a refused connection | `pending`, due again after the next pause of the schedule, with up to 10% jitter |
| the same after the last pause | `failed` |
| 410 Gone | `pending`, and the endpoint is disabled (see below) |

A redirect is not followed: it counts as a failed attempt with its status code, and
the endpoint's URL should be updated.

## Disabled endpoints

The relay disables an endpoint in two cases, and logs `endpoint disabled` with the
reason:

- **410 Gone.** The receiver says the URL is gone. The endpoint is disabled after that
  one attempt, with the reason `the endpoint answered 410 Gone`.
- **A failure streak.** The first failed attempt after a success sets
  `failing_since`; any success clears it. A failed attempt when `failing_since` is older
  than `RELAY_DISABLE_AFTER` disables the endpoint with the reason
  `no successful attempt since <time>`. With the default schedule a delivery gives up
  after about 28 hours, so a dead endpoint is disabled by the messages that keep coming
  after that, not by one old message.

A disabled endpoint gets no new deliveries. Its pending ones keep their attempt count
and wait; a delivery whose last attempt was the one that disabled the endpoint is
still `pending`. `endpoints enable` starts a new streak and the waiting deliveries go
out in the next round. Messages queued while the endpoint was disabled are not sent to
it.

## Private addresses

Endpoint URLs come from customers, so by default the relay refuses to connect to:

| Range | Examples |
|---|---|
| loopback | `127.0.0.0/8`, `::1` |
| private networks | `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7` |
| link-local, where cloud metadata lives | `169.254.0.0/16` (`169.254.169.254`), `fe80::/10` |
| carrier-grade NAT, reserved, documentation, benchmarking | `100.64.0.0/10`, `240.0.0.0/4`, `192.0.2.0/24`, `198.18.0.0/15` |
| unspecified, multicast, broadcast | `0.0.0.0/8`, `::`, `224.0.0.0/4`, `ff00::/8` |

IPv4-mapped IPv6 (`::ffff:10.0.0.1`) and NAT64 (`64:ff9b::a9fe:a9fe`) addresses are
checked as the IPv4 address inside. The check runs on the address the relay is about
to connect to, after DNS. A name that resolves to `10.0.0.5` is refused, also when it
resolved to a public address a moment before (DNS rebinding). The attempt is recorded
with no status code and an error such as `blocked: 127.0.0.1 is not a public address
(loopback)`, and retried like any failure.

The relay does not use `HTTP_PROXY` or `HTTPS_PROXY`: through a proxy, the address it
connects to is the proxy's, and the check would mean nothing.

Set `RELAY_ALLOW_PRIVATE_NETWORKS=true` only when every endpoint URL is yours, for
example when the receivers are your own services in the same network.

## Guarantees and limits

- **At least once.** A relay that dies after sending and before recording leaves the
  delivery claimed until the lease runs out; then it is sent again under the same
  `webhook-id`. Receivers drop repeats by that id.
- **No order.** Two messages to one endpoint can arrive in any order, and a retried
  message arrives after newer ones. Put a version or a time in the payload if the
  receiver needs order.
- **Several relays** can run against one database. They never take the same row at
  once; there is no leader.
- **Limits.** A request takes at most `RELAY_TIMEOUT`, a response body is read up to
  `RELAY_RESPONSE_LIMIT` bytes, and redirects are not followed.
- **One slow endpoint** holds at most `RELAY_ENDPOINT_CONCURRENCY` requests, counted
  over all relays, and the other endpoints get the rest. Its own deliveries wait for a
  free slot, which shows in `delivery_lag`. A relay claims only as many deliveries as
  it has free slots, so a claimed delivery is sent at once. See
  [ADR 0002](adr/0002-requests-in-flight-are-limited-per-endpoint.md).
- **Endpoints added later** get only messages dispatched after they were added.
  **Disabled endpoints** get no new deliveries; their pending ones wait and go out after
  `endpoints enable`.

## Replay

`replay` puts deliveries back in the queue. The relay sends them in its next round,
under the same `webhook-id`, with a fresh retry schedule. Deliveries that are pending
already are left alone: they go out anyway, and resetting one that is in flight would
send it twice.

```
# send two messages again to every endpoint that got them
$ webhook-outbox-relay replay msg_01J... msg_01K...
requeued 2, created 0

# send a message to one endpoint, also one that never got it
$ webhook-outbox-relay replay -endpoint ep_2bq7v3k4... msg_01J...
requeued 0, created 1

# an endpoint was down since Friday 9:00 and is fixed now
$ webhook-outbox-relay endpoints enable ep_2bq7v3k4...
$ webhook-outbox-relay replay -endpoint ep_2bq7v3k4... -since 2026-11-20T09:00:00Z
requeued 37, created 112
```

The last form is for catching an endpoint up after an outage. For messages created
at or after `-since` (RFC 3339, or a duration before now such as `72h`), it requeues
the endpoint's failed deliveries and creates deliveries for the messages it missed
while it was disabled. Deliveries that succeeded are not sent again. The endpoint's
customer and event types are respected in every form: replay does not send a message
to an endpoint that would not have got it. Messages the relay has not dispatched yet
are skipped; dispatch handles them.

A replay to a disabled endpoint is allowed and prints a note: the deliveries wait
until `endpoints enable`.

## Metrics

Counters and the histogram count what one relay process did; sum them over replicas.
The backlog gauges are read from the database on every scrape and are the same on
every replica; take `max`.

| Metric | Type | Meaning |
|---|---|---|
| `webhook_outbox_attempts_total{result, code}` | counter | attempts by `result` (`succeeded`, `retrying`, `failed` for the last one) and status class (`2xx`..`5xx`, `none` for no response); a 410 is `retrying`, because its delivery stays pending |
| `webhook_outbox_attempt_duration_seconds` | histogram | from connecting to reading the response |
| `webhook_outbox_dispatched_messages_total` | counter | messages turned into deliveries |
| `webhook_outbox_endpoints_disabled_total{reason}` | counter | `gone` (410) or `failing` (streak past `RELAY_DISABLE_AFTER`) |
| `webhook_outbox_pending_deliveries{state}` | gauge | `due` (should go out now), `waiting` (for a retry, or in flight), `endpoint_disabled` |
| `webhook_outbox_delivery_lag_seconds` | gauge | how long the most overdue delivery to an enabled endpoint has been due |
| `webhook_outbox_undispatched_messages` | gauge | messages not turned into deliveries yet |
| `webhook_outbox_dispatch_lag_seconds` | gauge | age of the oldest undispatched message |
| `webhook_outbox_disabled_endpoints` | gauge | endpoints that are disabled |
| `webhook_outbox_backlog_up` | gauge | 1 if the backlog query worked |
| `webhook_outbox_relay_info{version}` | gauge | 1 |

Both lags stay near zero while the relays keep up, whatever the customers' servers
do: a delivery waiting for its next retry is not due, so a slow or dead endpoint does
not move them. They grow when there are too few relays, or none.

```yaml
- alert: WebhookRelayBehind
  expr: max(webhook_outbox_delivery_lag_seconds) > 60 or max(webhook_outbox_dispatch_lag_seconds) > 60
  for: 5m
- alert: WebhookRelayBlind
  expr: max(webhook_outbox_backlog_up) == 0 or absent(webhook_outbox_backlog_up)
  for: 5m
- alert: WebhookDeliveriesFailing
  # Share of attempts that got no 2xx. Customers' servers fail all the time, so the
  # threshold is a matter of taste; page on the lags, not on this.
  expr: |
    sum(rate(webhook_outbox_attempts_total{result!="succeeded"}[15m]))
      / sum(rate(webhook_outbox_attempts_total[15m])) > 0.5
  for: 30m
```
