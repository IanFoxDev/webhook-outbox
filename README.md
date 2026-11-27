# webhook-outbox

Webhooks to your customers, queued in your application's database transaction and
delivered by a separate relay. Signed per [Standard Webhooks](https://www.standardwebhooks.com/),
retried on a schedule, with a log of every attempt. One PostgreSQL, nothing else.

## The bug it removes

The usual code calls the customer's URL, or pushes a job, right after the commit:

```php
$pdo->commit();
$http->post($merchant->webhookUrl, $payload);   // the process dies here: the order is
                                                // paid, the merchant never hears of it
```

Moved before the commit, it fails the other way: the merchant is told about an order
that then rolls back. Here the webhook is a row inserted in the same transaction as
the order. It exists if and only if the order does, and the relay sends it after the
commit, as many times as it takes. See
[ADR 0001](docs/adr/0001-messages-are-queued-in-the-application-transaction.md).

## In short

```php
$pdo->beginTransaction();
$pdo->prepare('UPDATE orders SET status = ? WHERE id = ?')->execute(['paid', 'A-17']);
$webhooks->send('acme', 'order.paid', ['order' => 'A-17', 'amount' => 5000]);
$pdo->commit();
```

The relay, running next to your application, sends it to every endpoint customer
`acme` registered for `order.paid`. On their side:

```php
$event = (new Webhook($secret))->verify($body, $headers);
```

## Try it

```bash
cd examples/shop
docker compose up -d --build --wait
curl -X POST localhost:8000/orders -d customer=flaky -d total=2500
curl localhost:8001/log
```

A shop, the relay, and a receiver whose endpoints answer slowly, fail twice and
recover, hang, stay down or answer 410. `docker compose run --rm --build e2e` checks
all of them; [examples/shop](examples/shop) explains what you see.

## What the relay does

- Signs every request with the endpoint's secret (`webhook-id`, `webhook-timestamp`,
  `webhook-signature`), so receivers verify it with any Standard Webhooks library.
  `endpoints rotate` changes a secret without downtime: for a day both sign.
- Retries a failure 7 times over about 28 hours, then marks the delivery failed.
  Every attempt is a row: status code, duration, error, the start of the response.
- Disables an endpoint that answers 410, or that has not taken a webhook for 5 days.
- Refuses to connect to loopback, private and cloud metadata addresses, checked after
  DNS, because endpoint URLs come from customers.
- Sends a message again, or catches an endpoint up after an outage, with `replay`.
- Serves `/metrics` with the backlog and how far behind the relays are.
- Runs as several replicas on one database, without a leader.

Delivery is at least once, without order between messages. Details and limits are in
[docs/relay.md](docs/relay.md).

## Install

```bash
composer require ianfoxdev/webhook-outbox
docker run --rm -e DATABASE_URL=postgres://... ghcr.io/ianfoxdev/webhook-outbox-relay migrate
docker run -d -e DATABASE_URL=postgres://... ghcr.io/ianfoxdev/webhook-outbox-relay run
```

PHP 8.3 or later with `pdo_pgsql`, PostgreSQL 14 or later. The image is built for
linux/amd64 and linux/arm64.

## Docs

- [PHP: queue and verify](docs/php.md)
- [Schema and endpoints](docs/schema.md)
- [Relay: settings, retries, replay, metrics](docs/relay.md)
- [Example: a shop and a receiver that fails](examples/shop)

## When to use something else

[Svix](https://github.com/svix/svix-webhooks) (MIT) and
[Hookdeck Outpost](https://github.com/hookdeck/outpost) (Apache 2.0) are webhook
servers: your application calls their API or queue, and they run with Redis next to
PostgreSQL. Take one of them if you want a customer portal, producers in many
languages, or a hosted option. This project is smaller on purpose: messages come only
from a table in your own database, written in your transaction, and the relay needs
nothing but that database.

## Layout

- The PHP package `ianfoxdev/webhook-outbox` is at the root: it queues messages and
  verifies signatures on the receiving side.
- The relay is a Go program in [relay/](relay), shipped as a Docker image.

One version number covers both.

## License

MIT
