// Command webhook-outbox-relay delivers the webhooks an application queued in its
// database.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/schema"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `usage: webhook-outbox-relay <command>

  run        deliver queued webhooks until stopped
  migrate    create or upgrade the webhook_ tables
  endpoints  add, list, disable and enable customers' endpoints
  replay     send messages again, or catch an endpoint up after an outage
  healthcheck
             exit 0 if the relay on RELAY_HTTP_ADDR answers /healthz
  version    print the version

The database is taken from DATABASE_URL (postgres://user:password@host/db).
run reads RELAY_POLL_INTERVAL (1s), RELAY_BATCH_SIZE (100), RELAY_CONCURRENCY (16),
RELAY_ENDPOINT_CONCURRENCY (4 requests in flight to one endpoint; 0 for no limit),
RELAY_TIMEOUT (15s), RELAY_RESPONSE_LIMIT (4096 bytes) and RELAY_RETRY_SCHEDULE, the
pause after each failed attempt (5s,5m,30m,2h,5h,10h,10h; "none" for one attempt).
An endpoint is disabled when it answers 410 Gone, or when no attempt has succeeded
for RELAY_DISABLE_AFTER (5d; "never" to keep it). Requests to loopback, private and
link-local addresses are refused unless RELAY_ALLOW_PRIVATE_NETWORKS=true.
/metrics, /healthz and /readyz are served on RELAY_HTTP_ADDR (:8080).
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "migrate":
		return withDB(ctx, getenv, stderr, func(conn *pgx.Conn) error {
			applied, err := schema.Migrate(ctx, conn)
			for _, name := range applied {
				fmt.Fprintln(stdout, "applied", name)
			}
			if err == nil && len(applied) == 0 {
				fmt.Fprintln(stdout, "up to date")
			}
			return err
		})
	case "run":
		return runRelay(ctx, getenv, stderr)
	case "endpoints":
		return endpoints(ctx, args[1:], getenv, stdout, stderr)
	case "healthcheck":
		return healthcheck(ctx, getenv, stderr)
	case "replay":
		return replayCmd(ctx, args[1:], getenv, stdout, stderr)
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

// withDB connects to DATABASE_URL, runs fn and reports its error. Exit code 1 means the
// command failed.
func withDB(ctx context.Context, getenv func(string) string, stderr io.Writer, fn func(*pgx.Conn) error) int {
	url := getenv("DATABASE_URL")
	if url == "" {
		fmt.Fprintln(stderr, "DATABASE_URL is not set")
		return 2
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		fmt.Fprintln(stderr, "connect:", err)
		return 1
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if err := fn(conn); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
