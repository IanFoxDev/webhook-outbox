# PHP

```
composer require ianfoxdev/webhook-outbox
```

PHP 8.3 or later. Queueing needs `ext-pdo_pgsql`; verifying needs nothing.

## Queue a webhook

```php
use IanFoxDev\WebhookOutbox\WebhookOutbox;

$webhooks = new WebhookOutbox($pdo);

$pdo->beginTransaction();
$pdo->prepare('UPDATE orders SET status = ? WHERE id = ?')->execute(['paid', 'A-17']);
$webhooks->send('acme', 'order.paid', ['order' => 'A-17', 'amount' => 5000]);
$pdo->commit();
```

`send()` inserts one row into `webhook_messages` and returns its id. If the
transaction rolls back, the row goes with it and nothing is sent. If it commits, the
relay delivers the message to every enabled endpoint of customer `acme` that takes
`order.paid`. Outside a transaction `send()` throws `NoActiveTransaction`: there, a
webhook could go out for a change that is then rolled back.

The body is a Standard Webhooks payload:

```json
{"type":"order.paid","timestamp":"2026-11-25T09:06:36.138Z","data":{"order":"A-17","amount":5000}}
```

`data` is an array or a `JsonSerializable`; an empty array is sent as `{}`. The
timestamp is the time of `send()`, in UTC with milliseconds.

The message id is `msg_` and a ULID, so ids sort by time. It is sent as `webhook-id`
on every attempt, and receivers drop repeats by it. You can pass your own as the
fourth argument, such as `order:A-17:paid`: 1 to 128 characters of letters, digits
and `_ . : -`. A second message with the same id fails the insert with a unique
violation, and in PostgreSQL that aborts the whole transaction. Pass your own id only
where a repeat is a bug you want to see.

### Laravel and Doctrine

Both run on PDO, and their transactions are PDO transactions, so `send()` sees them:

```php
// Laravel
$webhooks = new WebhookOutbox(DB::connection()->getPdo());

DB::transaction(function () use ($webhooks, $order) {
    $order->update(['status' => 'paid']);
    $webhooks->send($order->merchant_id, 'order.paid', ['order' => $order->id]);
});

// Doctrine DBAL with the pdo_pgsql driver
$webhooks = new WebhookOutbox($connection->getNativeConnection());
```

### Tests

Type-hint `IanFoxDev\WebhookOutbox\Sender` in your services, and pass
`Testing\InMemorySender` in unit tests:

```php
$sender = new InMemorySender();
(new PayOrder($sender))($order);
self::assertCount(1, $sender->ofType('order.paid'));
```

## Verify a webhook

On the receiving side, with the endpoint's secret from `endpoints add` or
`endpoints secret`:

```php
use IanFoxDev\WebhookOutbox\Exception\VerificationFailed;
use IanFoxDev\WebhookOutbox\Webhook;

$webhook = new Webhook($secret);

// Symfony: $request->getContent(), $request->headers->all()
// Laravel: $request->getContent(), $request->headers->all()
// PSR-7:   (string) $request->getBody(), $request->getHeaders()
try {
    $event = $webhook->verify($body, $headers);
} catch (VerificationFailed $e) {
    return new Response('', 400);
}
// $event['type'], $event['data']; drop it if webhook-id was seen before.
```

`verify()` checks the HMAC-SHA256 signature over `id.timestamp.body` and that
`webhook-timestamp` is within 5 minutes of now, both ways (the second constructor
argument changes that). It returns the decoded body. Pass the raw body: the signature
covers its exact bytes, and decoding and encoding it again changes them. Header names
match in any case, and `webhook-signature` may carry several signatures; one valid
`v1` is enough. It works with any sender that follows Standard Webhooks. During a
secret rotation (`endpoints rotate`, see [schema.md](schema.md#rotating-a-secret)) the
relay signs with the old and the new secret, so the receiver can switch to the new one
whenever it is deployed.

`sign()` makes the three headers for a body, for tests of your receiver:

```php
$headers = $webhook->sign('msg_1', time(), $body);
```
