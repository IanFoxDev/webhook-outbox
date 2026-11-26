package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/admin"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/metrics"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/relay"
)

// config reads the relay settings from the environment. Unset variables keep the
// defaults of relay.Config.
func config(getenv func(string) string) (relay.Config, error) {
	var c relay.Config
	var err error
	duration := func(name string, dst *time.Duration) {
		if v := getenv(name); v != "" && err == nil {
			d, e := time.ParseDuration(v)
			if e != nil || d <= 0 {
				err = fmt.Errorf("%s: %q is not a positive duration such as 500ms or 2s", name, v)
				return
			}
			*dst = d
		}
	}
	number := func(name string, dst *int) {
		if v := getenv(name); v != "" && err == nil {
			n, e := strconv.Atoi(v)
			if e != nil || n <= 0 {
				err = fmt.Errorf("%s: %q is not a positive number", name, v)
				return
			}
			*dst = n
		}
	}
	duration("RELAY_POLL_INTERVAL", &c.PollInterval)
	duration("RELAY_TIMEOUT", &c.Timeout)
	number("RELAY_BATCH_SIZE", &c.BatchSize)
	number("RELAY_CONCURRENCY", &c.Concurrency)
	number("RELAY_RESPONSE_LIMIT", &c.ResponseLimit)
	if v := getenv("RELAY_ENDPOINT_CONCURRENCY"); v != "" && err == nil {
		n, e := strconv.Atoi(v)
		if e != nil || n < 0 {
			return c, fmt.Errorf("RELAY_ENDPOINT_CONCURRENCY: %q is not a number; 0 turns the limit off", v)
		}
		c.EndpointConcurrency = n
		if n == 0 {
			c.EndpointConcurrency = -1
		}
	}
	if v := getenv("RELAY_DISABLE_AFTER"); v != "" && err == nil {
		d, ok := days(v)
		switch {
		case v == "never":
			c.DisableAfter = -1
		case ok:
			c.DisableAfter = d
		default:
			return c, fmt.Errorf("RELAY_DISABLE_AFTER: %q is not a number of days such as 5d, a duration such as 36h, or never", v)
		}
	}
	switch v := getenv("RELAY_ALLOW_PRIVATE_NETWORKS"); v {
	case "", "false":
	case "true":
		c.AllowPrivateNetworks = true
	default:
		return c, fmt.Errorf("RELAY_ALLOW_PRIVATE_NETWORKS: %q is not true or false", v)
	}
	if v := getenv("RELAY_RETRY_SCHEDULE"); v != "" && err == nil {
		c.Schedule = []time.Duration{}
		if v != "none" {
			for _, part := range strings.Split(v, ",") {
				d, e := time.ParseDuration(strings.TrimSpace(part))
				if e != nil || d < 0 {
					return c, fmt.Errorf("RELAY_RETRY_SCHEDULE: %q is not a duration", part)
				}
				c.Schedule = append(c.Schedule, d)
			}
		}
	}
	return c, err
}

// days parses 5d as five days and anything else as a Go duration. It accepts only
// positive values.
func days(v string) (time.Duration, bool) {
	if n, found := strings.CutSuffix(v, "d"); found {
		d, err := strconv.Atoi(n)
		return time.Duration(d) * 24 * time.Hour, err == nil && d > 0
	}
	d, err := time.ParseDuration(v)
	return d, err == nil && d > 0
}

func httpAddr(getenv func(string) string) string {
	if v := getenv("RELAY_HTTP_ADDR"); v != "" {
		return v
	}
	return ":8080"
}

// healthcheck exits 0 if the relay in this container answers /healthz. The image has
// no shell or curl, so a Docker healthcheck runs the binary itself.
func healthcheck(ctx context.Context, getenv func(string) string, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := admin.Probe(ctx, httpAddr(getenv)); err != nil {
		fmt.Fprintln(stderr, "healthcheck:", err)
		return 1
	}
	return 0
}

func runRelay(ctx context.Context, getenv func(string) string, stderr io.Writer) int {
	cfg, err := config(getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	url := getenv("DATABASE_URL")
	if url == "" {
		fmt.Fprintln(stderr, "DATABASE_URL is not set")
		return 2
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintln(stderr, "connect:", err)
		return 1
	}
	defer pool.Close()
	cfg.Logger = slog.New(slog.NewJSONHandler(stderr, nil))
	cfg.UserAgent = "webhook-outbox/" + version
	m := metrics.New(pool, version, cfg.Logger)
	cfg.Metrics = m
	cfg = cfg.Defaults()
	cfg.Logger.Info("relay started", "version", version, "poll_interval", cfg.PollInterval.String(),
		"batch_size", cfg.BatchSize, "concurrency", cfg.Concurrency, "timeout", cfg.Timeout.String(), "attempts", len(cfg.Schedule)+1,
		"disable_after", cfg.DisableAfter.String(), "allow_private_networks", cfg.AllowPrivateNetworks)
	// The admin server stops the relay if it cannot start, so a busy port fails the
	// start instead of leaving a relay nobody can see.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	addr := httpAddr(getenv)
	checks := []admin.Check{{Name: "postgres", Ping: pool.Ping}}
	serveErr := make(chan error, 1)
	go func() {
		err := admin.Serve(ctx, addr, admin.Handler(m.Registry, checks))
		if err != nil {
			stop()
		}
		serveErr <- err
	}()
	cfg.Logger.Info("serving /metrics, /healthz and /readyz", "addr", addr)
	runErr := relay.New(pool, cfg).Run(ctx)
	stop()
	if err := errors.Join(runErr, <-serveErr); err != nil {
		cfg.Logger.Error("relay stopped", "error", err)
		return 1
	}
	cfg.Logger.Info("relay stopped")
	return 0
}
