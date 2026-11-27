<?php

declare(strict_types=1);

namespace IanFoxDev\WebhookOutbox\Tests\Unit;

use IanFoxDev\WebhookOutbox\Exception\InvalidArgument;
use IanFoxDev\WebhookOutbox\Exception\VerificationFailed;
use IanFoxDev\WebhookOutbox\Webhook;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WebhookTest extends TestCase
{
    // The vector the relay's Go tests use, so both sides sign the same way.
    private const SECRET = 'whsec_dGVzdC1zZWNyZXQ=';
    private const ID = 'evt_1';
    private const TIMESTAMP = 1790330400;
    private const BODY = '{"id":"evt_1","type":"payment.captured"}';
    private const SIGNATURE = 'v1,DpPc9pmsF6HhjneOVQvZ7vDaVsOWIJZaIjvPPCJpZs8=';

    /**
     * @return array<string, string|list<string>>
     */
    private static function headers(string $signature = self::SIGNATURE): array
    {
        return ['webhook-id' => self::ID, 'webhook-timestamp' => (string) self::TIMESTAMP, 'webhook-signature' => $signature];
    }

    public function testSignMatchesTheRelay(): void
    {
        self::assertSame(self::headers(), (new Webhook(self::SECRET))->sign(self::ID, self::TIMESTAMP, self::BODY));
    }

    public function testVerifyReturnsTheBody(): void
    {
        $event = (new Webhook(self::SECRET))->verify(self::BODY, self::headers(), self::TIMESTAMP + 60);

        self::assertSame(['id' => 'evt_1', 'type' => 'payment.captured'], $event);
    }

    public function testHeadersInAnyCaseAndAsLists(): void
    {
        // Symfony's $request->headers->all() and PSR-7 getHeaders() give lists.
        $headers = [
            'Webhook-Id' => [self::ID],
            'WEBHOOK-TIMESTAMP' => [(string) self::TIMESTAMP],
            'webhook-signature' => ['v1,c29tZXRoaW5nIGVsc2U= ' . self::SIGNATURE],
        ];

        self::assertSame('evt_1', (new Webhook(self::SECRET))->verify(self::BODY, $headers, self::TIMESTAMP)['id']);
    }

    public function testEitherSecretDuringARotation(): void
    {
        // While an endpoint's secret rotates, the relay signs with the new secret first
        // and the old one after it. A receiver holding either accepts the request.
        $old = 'whsec_' . base64_encode('old-secret');
        $previous = (new Webhook($old))->sign(self::ID, self::TIMESTAMP, self::BODY)['webhook-signature'];
        $headers = self::headers(self::SIGNATURE . ' ' . $previous);

        foreach ([self::SECRET, $old] as $secret) {
            self::assertSame('evt_1', (new Webhook($secret))->verify(self::BODY, $headers, self::TIMESTAMP)['id']);
        }
    }

    public function testSecretWithoutPrefix(): void
    {
        $webhook = new Webhook(substr(self::SECRET, 6));

        self::assertSame('evt_1', $webhook->verify(self::BODY, self::headers(), self::TIMESTAMP)['id']);
    }

    /**
     * @return iterable<string, array{string, array<string, string|list<string>>, int, string}>
     */
    public static function rejected(): iterable
    {
        $h = self::headers();
        yield 'changed body' => [self::BODY . ' ', $h, self::TIMESTAMP, 'No valid v1 signature'];
        yield 'other id' => [self::BODY, ['webhook-id' => 'evt_2'] + $h, self::TIMESTAMP, 'No valid v1 signature'];
        yield 'wrong secret' => [self::BODY, self::headers('v1,' . base64_encode(str_repeat('x', 32))), self::TIMESTAMP, 'No valid v1 signature'];
        yield 'v2 only' => [self::BODY, self::headers('v2,' . substr(self::SIGNATURE, 3)), self::TIMESTAMP, 'No valid v1 signature'];
        yield 'too old' => [self::BODY, $h, self::TIMESTAMP + 301, '301 seconds away'];
        yield 'from the future' => [self::BODY, $h, self::TIMESTAMP - 301, '301 seconds away'];
        yield 'timestamp in ms' => [self::BODY, ['webhook-timestamp' => '1790330400000'] + $h, self::TIMESTAMP, 'not a Unix time'];
        yield 'no signature' => [self::BODY, ['webhook-id' => self::ID, 'webhook-timestamp' => (string) self::TIMESTAMP], self::TIMESTAMP, 'webhook-signature header is missing'];
        yield 'empty id' => [self::BODY, ['webhook-id' => ' '] + $h, self::TIMESTAMP, 'webhook-id header is missing'];
    }

    /**
     * @param array<string, string|list<string>> $headers
     */
    #[DataProvider('rejected')]
    public function testRejects(string $body, array $headers, int $now, string $message): void
    {
        $this->expectException(VerificationFailed::class);
        $this->expectExceptionMessage($message);

        (new Webhook(self::SECRET))->verify($body, $headers, $now);
    }

    public function testSignedBodyThatIsNotAnObject(): void
    {
        $webhook = new Webhook(self::SECRET);

        $this->expectException(VerificationFailed::class);
        $this->expectExceptionMessage('not a JSON object');

        $webhook->verify('"text"', $webhook->sign('evt_1', self::TIMESTAMP, '"text"'), self::TIMESTAMP);
    }

    public function testBadSecret(): void
    {
        $this->expectException(InvalidArgument::class);

        new Webhook('whsec_not base64!');
    }
}
