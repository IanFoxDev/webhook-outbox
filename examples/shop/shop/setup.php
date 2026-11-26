<?php

declare(strict_types=1);

// One endpoint per receiver scenario. The customer is named after the scenario, so
// an order for customer "flaky" goes to http://receiver:8000/flaky only. /timeout is
// served by its own copy of the receiver, see compose.yaml.
//
// A real application would run `webhook-outbox-relay endpoints add` or insert the row
// from its settings page; here a few INSERTs keep the example to one PHP image.

const SCENARIOS = ['ok', 'slow', 'flaky', 'timeout', 'down', 'gone'];

$pdo = new PDO((string) getenv('DATABASE_DSN'), options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
if ((int) $pdo->query('SELECT count(*) FROM webhook_endpoints')->fetchColumn() > 0) {
    echo "endpoints already registered\n";
    exit(0);
}

$insert = $pdo->prepare('INSERT INTO webhook_endpoints (id, customer, url, secret, event_types, description)
    VALUES (?, ?, ?, ?, ?, ?)');
$secrets = [];
foreach (SCENARIOS as $scenario) {
    $id = 'ep_'.$scenario;
    $secret = 'whsec_'.base64_encode(random_bytes(32));
    $host = $scenario === 'timeout' ? 'hang' : 'receiver';
    $insert->execute([$id, $scenario, "http://$host:8000/$scenario", $secret, '{order.placed}', 'receiver scenario '.$scenario]);
    $secrets[$scenario] = $secret;
    echo "registered $id\n";
}
file_put_contents('/secrets/secrets.json', json_encode($secrets, JSON_THROW_ON_ERROR));
