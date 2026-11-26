# 0002. Requests in flight are limited per endpoint

Date: 2026-11-26. Status: accepted.

## Context

In 0.1 a relay claims the due deliveries with the oldest `next_attempt_at`, whatever
endpoint they go to. One customer whose endpoint takes 15 seconds to answer, with a few
hundred webhooks queued, gets every request the relay can make: all of
`RELAY_CONCURRENCY`, for `RELAY_TIMEOUT` each. Webhooks to every other customer wait
behind it, and the slow receiver gets more parallel requests than it can handle, which
makes it slower.

Two things were missing. A claimed delivery looked the same as one waiting for a retry
(both are pending with `next_attempt_at` in the future), so nobody could count the
requests in flight to an endpoint. And the claim query picked rows in one ordered scan,
so it could not take "at most N per endpoint": PostgreSQL does not allow window
functions in a query with `FOR UPDATE`.

## Decision

`RELAY_ENDPOINT_CONCURRENCY` (4 by default) caps the requests in flight to one endpoint,
counted over all relays sharing the database.

- A claim sets `webhook_deliveries.leased_until` to the end of its lease, and recording
  the attempt clears it. Requests in flight to an endpoint are its pending deliveries
  with `leased_until` in the future. A relay that dies leaves leases that run out, the
  same way its deliveries become due again.
- A claim is one transaction of two statements. The first takes the enabled endpoints
  with due deliveries, oldest due first, and locks their rows with
  `FOR NO KEY UPDATE SKIP LOCKED`. The second counts each one's requests in flight and
  claims up to the remainder of its due deliveries through `LATERAL ... LIMIT`.
- The second statement starts after the endpoint locks are held. Any relay that claimed
  for the same endpoint committed before releasing the lock, so the count sees its
  leases. In one statement the count would come from a snapshot taken before the lock
  and could miss them.
- `0` turns the limit off and keeps the 0.1 claim.

## Consequences

- One slow endpoint holds at most 4 requests; the rest of the batch goes to others.
  Its own deliveries wait for a free slot and are sent later, which counts towards
  `delivery_lag`. That is the point: the lag shows a receiver that cannot keep up.
- A claim visits every enabled endpoint once, through an index on
  `(endpoint_id, next_attempt_at)`. With tens of thousands of endpoints this is the
  cost of a round, and it is paid once per poll.
- Endpoint rows are locked for the length of a claim. `endpoints disable` and the
  health update after an attempt wait for it, or a claim skips an endpoint whose row is
  being updated at that moment and takes it next round.
- Relays from 0.1 on the same database do not set `leased_until`, so their requests are
  not counted. Upgrade all relays together.
- Migration 0002 builds two indexes on `webhook_deliveries`. Only the relay writes to
  that table, so the build pauses delivery, not the application.
