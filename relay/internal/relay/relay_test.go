package relay_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/endpoint"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/relay"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/schema"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/signing"
)

type env struct {
	t    *testing.T
	ctx  context.Context
	url  string
	pool *pgxpool.Pool
}

func setup(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	url := pgtest.URL(t)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := schema.Migrate(ctx, conn.Conn()); err != nil {
		t.Fatal(err)
	}
	return &env{t: t, ctx: ctx, url: url, pool: pool}
}

func (e *env) endpoint(customer, url string, types ...string) endpoint.Endpoint {
	e.t.Helper()
	conn, err := e.pool.Acquire(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer conn.Release()
	ep, err := endpoint.Add(e.ctx, conn.Conn(), endpoint.New{Customer: customer, URL: url, EventTypes: types})
	if err != nil {
		e.t.Fatal(err)
	}
	return ep
}

// message inserts a row the way an application does.
func (e *env) message(id, customer, typ, payload string) {
	e.t.Helper()
	if _, err := e.pool.Exec(e.ctx, `INSERT INTO webhook_messages (id, customer, type, payload) VALUES ($1, $2, $3, $4)`,
		id, customer, typ, payload); err != nil {
		e.t.Fatal(err)
	}
}

type state struct {
	status   string
	attempts int
}

// deliveries returns the delivery of each endpoint for a message.
func (e *env) deliveries(message string) map[string]state {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `SELECT endpoint_id, status, attempts FROM webhook_deliveries WHERE message_id = $1`, message)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]state{}
	for rows.Next() {
		var ep string
		var s state
		if err := rows.Scan(&ep, &s.status, &s.attempts); err != nil {
			e.t.Fatal(err)
		}
		out[ep] = s
	}
	return out
}

// attempts returns the recorded status codes and errors of a message's attempts.
func (e *env) attempts(message string) []string {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `SELECT COALESCE(a.status_code::text, 'none'), COALESCE(a.error, ''), length(COALESCE(a.response, ''))
		FROM webhook_attempts a JOIN webhook_deliveries d ON d.id = a.delivery_id
		WHERE d.message_id = $1 ORDER BY a.id`, message)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var code, errText string
		var n int
		if err := rows.Scan(&code, &errText, &n); err != nil {
			e.t.Fatal(err)
		}
		if errText != "" {
			code += ": " + errText
		}
		out = append(out, fmt.Sprintf("%s (%d bytes)", code, n))
	}
	return out
}

func (e *env) round(r *relay.Relay) {
	e.t.Helper()
	if _, err := r.Round(e.ctx); err != nil {
		e.t.Fatal(err)
	}
}

// fast retries at once, so a test can run the schedule with a few rounds.
var fast = relay.Config{AllowPrivateNetworks: true, Schedule: []time.Duration{0, 0}, Timeout: 2 * time.Second}

func TestDeliversSignedBody(t *testing.T) {
	e := setup(t)
	var got http.Header
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, body = r.Header.Clone(), string(b)
		_, _ = io.WriteString(w, "thanks")
	}))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL+"/hooks")
	payload := `{"type":"order.paid","data":{"order":"A-17","amount":5000}}`
	e.message("msg_1", "acme", "order.paid", payload)

	e.round(relay.New(e.pool, fast))

	key, _ := signing.Key(ep.Secret)
	if err := signing.Verify(got, key, []byte(body), time.Now(), time.Minute); err != nil {
		t.Fatalf("signature: %v, headers %v", err, got)
	}
	if body != payload || got.Get("Webhook-Id") != "msg_1" || got.Get("Content-Type") != "application/json" {
		t.Fatalf("body %q, headers %v", body, got)
	}
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"succeeded", 1}) {
		t.Fatalf("%v", d)
	}
	if a := e.attempts("msg_1"); len(a) != 1 || a[0] != "200 (6 bytes)" {
		t.Fatalf("%v", a)
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	r := relay.New(e.pool, fast)

	e.round(r)
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"pending", 1}) {
		t.Fatalf("after the 500: %v", d)
	}
	e.round(r)
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"succeeded", 2}) {
		t.Fatalf("after the 200: %v", d)
	}
	if a := e.attempts("msg_1"); strings.Join(a, ",") != "500 (0 bytes),200 (0 bytes)" {
		t.Fatalf("%v", a)
	}
}

func TestGivesUpAfterTheSchedule(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	r := relay.New(e.pool, fast)
	for range 5 {
		e.round(r)
	}
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"failed", 3}) || calls.Load() != 3 {
		t.Fatalf("%v after %d calls", d, calls.Load())
	}
}

func TestWaitsForTheNextAttempt(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	r := relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, Schedule: []time.Duration{time.Hour}})
	e.round(r)
	e.round(r)
	if calls.Load() != 1 {
		t.Fatalf("%d calls; the second is an hour away", calls.Load())
	}
	var wait time.Duration
	if err := e.pool.QueryRow(e.ctx, `SELECT EXTRACT(EPOCH FROM next_attempt_at - now())::bigint * 1000000000 FROM webhook_deliveries`).Scan(&wait); err != nil {
		t.Fatal(err)
	}
	if wait < 50*time.Minute || wait > 70*time.Minute {
		t.Fatalf("next attempt in %v", wait)
	}
}

func TestRetryAfter(t *testing.T) {
	e := setup(t)
	for _, tc := range []struct {
		name      string
		status    int
		header    string
		wantMin   time.Duration
		wantMax   time.Duration
		wantSaved bool
	}{
		{"seconds", http.StatusTooManyRequests, "120", 120 * time.Second, 120 * time.Second, true},
		{"http-date", http.StatusServiceUnavailable, "", 150 * time.Second, 180 * time.Second, true},
		{"cap", http.StatusServiceUnavailable, "999999999999999999999", 5 * time.Minute, 5 * time.Minute, true},
		{"shorter", http.StatusTooManyRequests, "1", time.Second, 3 * time.Second, true},
		{"invalid", http.StatusTooManyRequests, "tomorrow", time.Second, 3 * time.Second, true},
		{"other-status", http.StatusBadGateway, "120", time.Second, 3 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := tc.header
			if tc.name == "http-date" {
				header = time.Now().Add(3 * time.Minute).UTC().Format(http.TimeFormat)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", header)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			e.endpoint(tc.name, srv.URL)
			e.message(tc.name, tc.name, "order.paid", `{}`)
			e.round(relay.New(e.pool, relay.Config{
				AllowPrivateNetworks: true,
				Schedule:             []time.Duration{2 * time.Second, 5 * time.Minute},
			}))

			var wait time.Duration
			var saved string
			err := e.pool.QueryRow(e.ctx, `
				SELECT (EXTRACT(EPOCH FROM (d.next_attempt_at - d.updated_at)) * 1000000000)::bigint,
					COALESCE(a.error, '')
				FROM webhook_deliveries d JOIN webhook_attempts a ON a.delivery_id = d.id
				WHERE d.message_id = $1
			`, tc.name).Scan(&wait, &saved)
			if err != nil {
				t.Fatal(err)
			}
			if wait < tc.wantMin || wait > tc.wantMax {
				t.Errorf("retry delay %v, want %v through %v", wait, tc.wantMin, tc.wantMax)
			}
			if got := strings.Contains(saved, "Retry-After: "+header); got != tc.wantSaved {
				t.Errorf("stored error %q, want Retry-After header recorded: %v", saved, tc.wantSaved)
			}
			_, err = e.pool.Exec(e.ctx, "UPDATE webhook_deliveries SET status = 'failed' WHERE message_id = $1", tc.name)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDispatchFollowsCustomerTypeAndState(t *testing.T) {
	e := setup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	every := e.endpoint("acme", srv.URL)
	paidOnly := e.endpoint("acme", srv.URL, "order.paid")
	refunds := e.endpoint("acme", srv.URL, "order.paid", "order.refunded")
	disabled := e.endpoint("acme", srv.URL)
	other := e.endpoint("globex", srv.URL)
	if _, err := e.pool.Exec(e.ctx, `UPDATE webhook_endpoints SET disabled_at = now() WHERE id = $1`, disabled.ID); err != nil {
		t.Fatal(err)
	}
	e.message("msg_1", "acme", "order.refunded", `{}`)
	e.round(relay.New(e.pool, fast))
	d := e.deliveries("msg_1")
	if len(d) != 2 || d[every.ID].status != "succeeded" || d[refunds.ID].status != "succeeded" {
		t.Fatalf("%v; not for %s, %s, %s", d, paidOnly.ID, disabled.ID, other.ID)
	}
}

func TestDisabledEndpointKeepsItsDeliveries(t *testing.T) {
	e := setup(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	r := relay.New(e.pool, fast)
	if _, err := r.Dispatch(e.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE webhook_endpoints SET disabled_at = now() WHERE id = $1`, ep.ID); err != nil {
		t.Fatal(err)
	}
	e.round(r)
	if calls.Load() != 0 || e.deliveries("msg_1")[ep.ID].status != "pending" {
		t.Fatal("sent to a disabled endpoint")
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE webhook_endpoints SET disabled_at = NULL WHERE id = $1`, ep.ID); err != nil {
		t.Fatal(err)
	}
	e.round(r)
	if calls.Load() != 1 || e.deliveries("msg_1")[ep.ID].status != "succeeded" {
		t.Fatal("not sent after enable")
	}
}

func TestTimeoutAndRedirectAreFailures(t *testing.T) {
	e := setup(t)
	slow := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(time.Second) }))
	defer slow.Close()
	moved := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer moved.Close()
	a := e.endpoint("slow", slow.URL)
	b := e.endpoint("moved", moved.URL)
	e.message("msg_slow", "slow", "order.paid", `{}`)
	e.message("msg_moved", "moved", "order.paid", `{}`)
	e.round(relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, Timeout: 200 * time.Millisecond, Schedule: []time.Duration{time.Hour}}))

	if d := e.deliveries("msg_slow"); d[a.ID] != (state{"pending", 1}) {
		t.Fatalf("%v", d)
	}
	if at := e.attempts("msg_slow"); len(at) != 1 || !strings.HasPrefix(at[0], "none: ") || !strings.Contains(at[0], "Timeout") {
		t.Fatalf("%v", at)
	}
	var ms int
	if err := e.pool.QueryRow(e.ctx, `SELECT a.duration_ms FROM webhook_attempts a
		JOIN webhook_deliveries d ON d.id = a.delivery_id WHERE d.message_id = 'msg_slow'`).Scan(&ms); err != nil || ms < 200 || ms > 1000 {
		t.Fatalf("duration_ms %d, %v", ms, err)
	}
	if d := e.deliveries("msg_moved"); d[b.ID] != (state{"pending", 1}) {
		t.Fatalf("%v", d)
	}
	if at := e.attempts("msg_moved"); len(at) != 1 || !strings.HasPrefix(at[0], "302") {
		t.Fatalf("%v", at)
	}
}

func TestResponseIsCut(t *testing.T) {
	e := setup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, strings.Repeat("x", 10_000)+"\x00\xff")
	}))
	defer srv.Close()
	e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	e.round(relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, Schedule: []time.Duration{time.Hour}, ResponseLimit: 100}))
	if a := e.attempts("msg_1"); len(a) != 1 || a[0] != "400 (100 bytes)" {
		t.Fatalf("%v", a)
	}
}

// Three relays on one database send every delivery exactly once when every endpoint
// answers at once.
func TestParallelRelaysSendEachDeliveryOnce(t *testing.T) {
	e := setup(t)
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path+" "+r.Header.Get("Webhook-Id")]++
		mu.Unlock()
	}))
	defer srv.Close()
	e.endpoint("acme", srv.URL+"/a")
	e.endpoint("acme", srv.URL+"/b")
	const messages = 300
	for i := range messages {
		e.message(fmt.Sprintf("msg_%03d", i), "acme", "order.paid", `{}`)
	}

	ctx, cancel := context.WithCancel(e.ctx)
	var wg sync.WaitGroup
	for range 3 {
		pool, err := pgxpool.New(e.ctx, e.url)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		r := relay.New(pool, relay.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond, BatchSize: 20, Concurrency: 4})
		wg.Go(func() { _ = r.Run(ctx) })
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var done int
		if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM webhook_deliveries WHERE status = 'succeeded'`).Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == 2*messages {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d delivered", done, 2*messages)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2*messages {
		t.Fatalf("%d distinct requests, want %d", len(seen), 2*messages)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%s sent %d times", k, n)
		}
	}
	var attempts int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM webhook_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2*messages {
		t.Fatalf("%d attempts recorded", attempts)
	}
}

func TestPrivateAddressesAreBlockedByDefault(t *testing.T) {
	e := setup(t)
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)

	e.round(relay.New(e.pool, relay.Config{Schedule: []time.Duration{time.Hour}}))

	if called {
		t.Fatal("the relay reached 127.0.0.1")
	}
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"pending", 1}) {
		t.Fatalf("%v", d)
	}
	if at := e.attempts("msg_1"); len(at) != 1 || !strings.Contains(at[0], "loopback") {
		t.Fatalf("%v", at)
	}
}

// health reads the endpoint columns the relay keeps: the disabled reason ("" when
// enabled) and whether failing_since is set.
func (e *env) health(id string) (string, bool) {
	e.t.Helper()
	var reason *string
	var failing bool
	if err := e.pool.QueryRow(e.ctx, `SELECT CASE WHEN disabled_at IS NOT NULL THEN COALESCE(disabled_reason, '') END,
		failing_since IS NOT NULL FROM webhook_endpoints WHERE id = $1`, id).Scan(&reason, &failing); err != nil {
		e.t.Fatal(err)
	}
	if reason == nil {
		return "", failing
	}
	return *reason, failing
}

func (e *env) failingFor(id string, d time.Duration) {
	e.t.Helper()
	if _, err := e.pool.Exec(e.ctx, `UPDATE webhook_endpoints SET failing_since = now() - $2 * interval '1 microsecond'
		WHERE id = $1`, id, d.Microseconds()); err != nil {
		e.t.Fatal(err)
	}
}

func answering(t *testing.T, code int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestGoneDisablesTheEndpoint(t *testing.T) {
	e := setup(t)
	ep := e.endpoint("acme", answering(t, http.StatusGone))
	e.message("msg_1", "acme", "order.paid", `{}`)
	e.round(relay.New(e.pool, fast))

	if reason, _ := e.health(ep.ID); reason != "the endpoint answered 410 Gone" {
		t.Fatalf("reason %q", reason)
	}
	// The delivery keeps its place and waits for the endpoint to be enabled.
	if d := e.deliveries("msg_1"); d[ep.ID] != (state{"pending", 1}) {
		t.Fatalf("%v", d)
	}
	e.message("msg_2", "acme", "order.paid", `{}`)
	e.round(relay.New(e.pool, fast))
	if d := e.deliveries("msg_2"); len(d) != 0 {
		t.Fatalf("a disabled endpoint got a new delivery: %v", d)
	}
	if a := e.attempts("msg_1"); len(a) != 1 {
		t.Fatalf("%v", a)
	}
}

func TestFailureStreakDisablesTheEndpoint(t *testing.T) {
	e := setup(t)
	young := e.endpoint("young", answering(t, http.StatusInternalServerError))
	old := e.endpoint("old", answering(t, http.StatusInternalServerError))
	e.failingFor(young.ID, 4*24*time.Hour)
	e.failingFor(old.ID, 6*24*time.Hour)
	e.message("msg_young", "young", "order.paid", `{}`)
	e.message("msg_old", "old", "order.paid", `{}`)
	e.round(relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, Schedule: []time.Duration{time.Hour}}))

	if reason, failing := e.health(young.ID); reason != "" || !failing {
		t.Fatalf("young: reason %q, failing %v", reason, failing)
	}
	if reason, _ := e.health(old.ID); !strings.HasPrefix(reason, "no successful attempt since ") {
		t.Fatalf("old: reason %q", reason)
	}
}

func TestFailureStreakStartsAndEnds(t *testing.T) {
	e := setup(t)
	var fail atomic.Bool
	fail.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	ep := e.endpoint("acme", srv.URL)
	e.message("msg_1", "acme", "order.paid", `{}`)
	r := relay.New(e.pool, fast)

	e.round(r)
	if reason, failing := e.health(ep.ID); reason != "" || !failing {
		t.Fatalf("after a failure: reason %q, failing %v", reason, failing)
	}
	fail.Store(false)
	e.round(r)
	if reason, failing := e.health(ep.ID); reason != "" || failing {
		t.Fatalf("after a success: reason %q, failing %v", reason, failing)
	}
}

func TestNegativeDisableAfterNeverDisables(t *testing.T) {
	e := setup(t)
	ep := e.endpoint("acme", answering(t, http.StatusInternalServerError))
	e.failingFor(ep.ID, 365*24*time.Hour)
	e.message("msg_1", "acme", "order.paid", `{}`)
	e.round(relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, DisableAfter: -1, Schedule: []time.Duration{time.Hour}}))
	if reason, _ := e.health(ep.ID); reason != "" {
		t.Fatalf("reason %q", reason)
	}
}
