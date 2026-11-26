# Example: a shop and a receiver that fails

A shop in plain PHP queues `order.placed` in the transaction that creates the order.
The relay from this repository delivers it to a receiver whose endpoints behave the
way real ones do: answer at once, answer slowly, fail twice and recover, hang, stay
down, or say the endpoint is gone.

| Service | What it is | Port on the host |
|---|---|---|
| `shop` | `POST /orders` with `customer` and `total`, see [shop/index.php](shop/index.php) | 8000 |
| `receiver` | one endpoint per scenario, checks every signature, `GET /log`, see [receiver/index.php](receiver/index.php) | 8001 |
| `hang` | the same receiver for `/timeout` only, so a hanging request does not hold up the others | |
| `relay` | built from [../../relay](../../relay), `/metrics` | 8080 |
| `postgres` | the shop's database with the `webhook_` tables and `orders` | |

Each scenario is a customer with one endpoint, so the customer of an order picks
what happens to its webhook:

| Customer | Endpoint answers | Ends as |
|---|---|---|
| `ok` | 200 | succeeded, 1 attempt |
| `slow` | 200 after 1 second | succeeded, 1 attempt |
| `flaky` | 500, 500, then 200 | succeeded, 3 attempts |
| `timeout` | nothing within the relay's 2 second timeout | failed, 3 attempts |
| `down` | 500 every time | failed, 3 attempts |
| `gone` | 410 | endpoint disabled, delivery waits for `endpoints enable` |

The relay here retries after 1 second and again after 1 second (`RELAY_RETRY_SCHEDULE=1s,1s`),
so a run takes seconds. The default schedule spreads 8 attempts over about 28 hours.

## Run

```bash
docker compose up -d --build --wait
docker compose run --rm --build e2e
```

[tests/e2e.php](tests/e2e.php) places one order per customer and one checkout that
fails after the insert, then checks the receiver's log against `webhook_attempts`:

```
ok   failed checkout left no order and no message
ok   every delivery settled within 30 seconds
ok   receiver verified the signature of every request
ok   every request carried the message id from send() as webhook-id
ok   flaky: 500, 500, then 200
ok   timeout: three attempts without a response, failed
ok   gone: endpoint disabled for the 410
...
all checks passed
```

It expects a fresh stack: run `docker compose down -v` before running it again.
Ports taken on your machine? `SHOP_PORT=18000 RECEIVER_PORT=18001 RELAY_PORT=18080 docker compose up -d --build --wait`.

By hand:

```bash
curl -X POST localhost:8000/orders -d customer=flaky -d total=2500
docker compose exec postgres psql -U shop -c 'SELECT d.message_id, d.status, a.status_code, a.duration_ms
  FROM webhook_attempts a JOIN webhook_deliveries d ON d.id = a.delivery_id ORDER BY a.id'
curl localhost:8001/log
docker compose run --rm relay endpoints list
```

```
ID          CUSTOMER  URL                           TYPES         STATUS
ep_ok       ok        http://receiver:8000/ok       order.placed  enabled
...
ep_gone     gone      http://receiver:8000/gone     order.placed  disabled: the endpoint answered 410 Gone
```

A checkout with `-d fail=1` throws after `send()`, the way a timed out payment call
would, and answers 503. The order and the message are rolled back together, and the
receiver gets nothing.

```bash
docker compose down -v
```

## What differs from a real setup

- [shop/setup.php](shop/setup.php) inserts the endpoints itself and writes their
  secrets to a volume the receiver reads. In a real setup you add endpoints with
  `webhook-outbox-relay endpoints add` or from your application's settings page, and
  the customer copies the secret from there.
- `RELAY_ALLOW_PRIVATE_NETWORKS=true`, because the receiver is on the compose network.
  Leave it off when the endpoints belong to customers.
- The shop runs on PHP's built-in server. The package needs nothing from a framework;
  [docs/php.md](../../docs/php.md) shows Laravel and Doctrine.
