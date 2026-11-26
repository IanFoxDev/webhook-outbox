package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/pgtest"
)

func TestConfig(t *testing.T) {
	c, err := config(func(k string) string {
		return map[string]string{
			"RELAY_POLL_INTERVAL":          "250ms",
			"RELAY_BATCH_SIZE":             "50",
			"RELAY_CONCURRENCY":            "4",
			"RELAY_ENDPOINT_CONCURRENCY":   "2",
			"RELAY_TIMEOUT":                "5s",
			"RELAY_RESPONSE_LIMIT":         "512",
			"RELAY_RETRY_SCHEDULE":         "1s, 1m",
			"RELAY_ALLOW_PRIVATE_NETWORKS": "true",
			"RELAY_DISABLE_AFTER":          "3d",
		}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.PollInterval != 250*time.Millisecond || c.BatchSize != 50 || c.Concurrency != 4 || c.EndpointConcurrency != 2 || c.Timeout != 5*time.Second ||
		c.ResponseLimit != 512 || !c.AllowPrivateNetworks || c.DisableAfter != 72*time.Hour || !reflect.DeepEqual(c.Schedule, []time.Duration{time.Second, time.Minute}) {
		t.Fatalf("%+v", c)
	}
	none, err := config(func(k string) string {
		if k == "RELAY_RETRY_SCHEDULE" {
			return "none"
		}
		return ""
	})
	if err != nil || none.Schedule == nil || len(none.Schedule) != 0 {
		t.Fatalf("%+v, %v", none.Schedule, err)
	}
}

func TestDisableAfter(t *testing.T) {
	for v, want := range map[string]time.Duration{"never": -1, "36h": 36 * time.Hour, "1d": 24 * time.Hour} {
		c, err := config(func(k string) string {
			if k == "RELAY_DISABLE_AFTER" {
				return v
			}
			return ""
		})
		if err != nil || c.DisableAfter != want {
			t.Errorf("%s: %v, %v", v, c.DisableAfter, err)
		}
	}
}

func TestEndpointConcurrency(t *testing.T) {
	for v, want := range map[string]int{"": 0, "0": -1, "1": 1, "8": 8} {
		c, err := config(func(k string) string {
			if k == "RELAY_ENDPOINT_CONCURRENCY" {
				return v
			}
			return ""
		})
		if err != nil || c.EndpointConcurrency != want {
			t.Errorf("%q: %v, %v", v, c.EndpointConcurrency, err)
		}
	}
}

func TestConfigRefuses(t *testing.T) {
	for name, value := range map[string]string{
		"RELAY_POLL_INTERVAL":          "1",
		"RELAY_TIMEOUT":                "-1s",
		"RELAY_BATCH_SIZE":             "0",
		"RELAY_CONCURRENCY":            "many",
		"RELAY_ENDPOINT_CONCURRENCY":   "-1",
		"RELAY_RETRY_SCHEDULE":         "1s,soon",
		"RELAY_ALLOW_PRIVATE_NETWORKS": "yes",
		"RELAY_DISABLE_AFTER":          "0d",
	} {
		_, err := config(func(k string) string {
			if k == name {
				return value
			}
			return ""
		})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s=%s: %v", name, value, err)
		}
	}
}

func TestRunStopsWithTheContext(t *testing.T) {
	env := map[string]string{"DATABASE_URL": pgtest.URL(t), "RELAY_POLL_INTERVAL": "10ms", "RELAY_HTTP_ADDR": "127.0.0.1:0"}
	if code, _, errOut := cli(t, env, "migrate"); code != 0 {
		t.Fatal(errOut)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var out, errOut strings.Builder
	code := run(ctx, []string{"run"}, func(k string) string { return env[k] }, &out, &errOut)
	if code != 0 || !strings.Contains(errOut.String(), `"msg":"relay stopped"`) {
		t.Fatalf("exit %d\n%s", code, errOut.String())
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func get(url string) (int, string) {
	resp, err := http.Get(url) //nolint:noctx // a test against a local server
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// eventually polls the relay's /metrics until it contains want.
func eventually(t *testing.T, addr, want string) string {
	t.Helper()
	var body string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		var code int
		if code, body = get("http://" + addr + "/metrics"); code == http.StatusOK && strings.Contains(body, want) {
			return body
		}
	}
	t.Fatalf("/metrics never had %q:\n%s", want, body)
	return ""
}

func TestRunServesMetrics(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer receiver.Close()
	addr := freeAddr(t)
	env := map[string]string{
		"DATABASE_URL":                 pgtest.URL(t),
		"RELAY_POLL_INTERVAL":          "10ms",
		"RELAY_HTTP_ADDR":              addr,
		"RELAY_ALLOW_PRIVATE_NETWORKS": "true",
	}
	if code, _, errOut := cli(t, env, "migrate"); code != 0 {
		t.Fatal(errOut)
	}
	if code, _, errOut := cli(t, env, "endpoints", "add", "-customer", "acme", "-url", receiver.URL); code != 0 {
		t.Fatal(errOut)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	go func() {
		done <- run(ctx, []string{"run"}, func(k string) string { return env[k] }, io.Discard, io.Discard)
	}()
	defer func() {
		cancel()
		if code := <-done; code != 0 {
			t.Errorf("run exited %d", code)
		}
	}()

	eventually(t, addr, "webhook_outbox_backlog_up 1")
	if code, _, errOut := cli(t, env, "healthcheck"); code != 0 {
		t.Fatalf("healthcheck: %s", errOut)
	}
	if code, body := get("http://" + addr + "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz: %d %s", code, body)
	}
	conn, err := pgx.Connect(context.Background(), env["DATABASE_URL"])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), `INSERT INTO webhook_messages (id, customer, type, payload)
		VALUES ('msg_1', 'acme', 'order.paid', '{}')`); err != nil {
		t.Fatal(err)
	}
	body := eventually(t, addr, `webhook_outbox_attempts_total{code="2xx",result="succeeded"} 1`)
	for _, want := range []string{
		"webhook_outbox_dispatched_messages_total 1",
		"webhook_outbox_attempt_duration_seconds_count 1",
		`webhook_outbox_relay_info{version="dev"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no %q", want)
		}
	}
}

func TestRunFailsWhenThePortIsTaken(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	env := map[string]string{"DATABASE_URL": pgtest.URL(t), "RELAY_HTTP_ADDR": ln.Addr().String()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var errOut strings.Builder
	if code := run(ctx, []string{"run"}, func(k string) string { return env[k] }, io.Discard, &errOut); code != 1 || ctx.Err() != nil {
		t.Fatalf("exit %d, ctx %v\n%s", code, ctx.Err(), errOut.String())
	}
}

func TestHealthcheckWithoutARelay(t *testing.T) {
	if code, _, errOut := cli(t, map[string]string{"RELAY_HTTP_ADDR": freeAddr(t)}, "healthcheck"); code != 1 || errOut == "" {
		t.Fatalf("exit %d", code)
	}
}
