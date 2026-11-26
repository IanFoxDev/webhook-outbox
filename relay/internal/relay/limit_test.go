package relay_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/relay"
)

// stuck is a receiver that holds every request until released, and keeps the most
// requests it had at once.
type stuck struct {
	mu      sync.Mutex
	now     int
	most    int
	release chan struct{}
}

func newStuck() *stuck { return &stuck{release: make(chan struct{})} }

func (s *stuck) ServeHTTP(http.ResponseWriter, *http.Request) {
	s.mu.Lock()
	s.now++
	s.most = max(s.most, s.now)
	s.mu.Unlock()
	<-s.release
	s.mu.Lock()
	s.now--
	s.mu.Unlock()
}

func (s *stuck) inFlight() (now, most int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now, s.most
}

func (e *env) eventually(what string, cond func() bool) {
	e.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			e.t.Fatalf("waited 10s for %s", what)
		}
	}
}

func (e *env) count(sql string) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, sql).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// run starts n relays on their own pools and returns a function that stops them.
func (e *env) run(n int, cfg relay.Config) func() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.ctx)
	var wg sync.WaitGroup
	for range n {
		pool, err := pgxpool.New(e.ctx, e.url)
		if err != nil {
			e.t.Fatal(err)
		}
		r := relay.New(pool, cfg)
		wg.Go(func() { _ = r.Run(ctx); pool.Close() })
	}
	return func() { cancel(); wg.Wait() }
}

func TestSlowEndpointHoldsOnlyItsLimit(t *testing.T) {
	e := setup(t)
	slow := newStuck()
	slowSrv := httptest.NewServer(slow)
	defer slowSrv.Close()
	var fastGot sync.WaitGroup
	fastGot.Add(1)
	fastSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { fastGot.Done() }))
	defer fastSrv.Close()
	e.endpoint("acme", slowSrv.URL)
	e.endpoint("beta", fastSrv.URL)
	for i := range 20 {
		e.message(fmt.Sprintf("msg_%02d", i), "acme", "order.paid", `{}`)
	}

	stop := e.run(3, relay.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond, Concurrency: 16, Timeout: 20 * time.Second})
	// Release the receiver first: a stopping relay waits for its requests.
	defer func() { close(slow.release); stop() }()
	e.eventually("4 requests to the slow endpoint", func() bool { now, _ := slow.inFlight(); return now == 4 })

	// The relays are busy with the slow endpoint; a webhook to another customer queued
	// now still goes out at once.
	e.message("msg_beta", "beta", "order.paid", `{}`)
	done := make(chan struct{})
	go func() { fastGot.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the other endpoint waited for the slow one")
	}
	time.Sleep(200 * time.Millisecond)
	if _, most := slow.inFlight(); most != 4 {
		t.Fatalf("the slow endpoint had %d requests at once over 3 relays, want 4", most)
	}
}

func TestEveryDeliveryGoesOutAndLeasesAreCleared(t *testing.T) {
	e := setup(t)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	e.endpoint("acme", srv.URL)
	for i := range 50 {
		e.message(fmt.Sprintf("msg_%02d", i), "acme", "order.paid", `{}`)
	}
	stop := e.run(2, relay.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond, EndpointConcurrency: 2})
	e.eventually("50 deliveries", func() bool {
		return e.count(`SELECT count(*) FROM webhook_deliveries WHERE status = 'succeeded'`) == 50
	})
	stop()
	if n := e.count(`SELECT count(*) FROM webhook_deliveries WHERE leased_until IS NOT NULL`); n != 0 {
		t.Fatalf("%d leases left after recording", n)
	}
	if n := e.count(`SELECT count(*) FROM webhook_attempts`); n != 50 {
		t.Fatalf("%d attempts, want 50", n)
	}
}

func TestNoLimitWhenTurnedOff(t *testing.T) {
	e := setup(t)
	slow := newStuck()
	srv := httptest.NewServer(slow)
	defer srv.Close()
	e.endpoint("acme", srv.URL)
	for i := range 10 {
		e.message(fmt.Sprintf("msg_%02d", i), "acme", "order.paid", `{}`)
	}
	stop := e.run(1, relay.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond, Concurrency: 8, EndpointConcurrency: -1, Timeout: 20 * time.Second})
	defer func() { close(slow.release); stop() }()
	e.eventually("8 requests to one endpoint", func() bool { now, _ := slow.inFlight(); return now == 8 })
}

func TestLeasesOfOtherRelaysCount(t *testing.T) {
	e := setup(t)
	var mu sync.Mutex
	got := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { mu.Lock(); got++; mu.Unlock() }))
	defer srv.Close()
	e.endpoint("acme", srv.URL)
	for i := range 3 {
		e.message(fmt.Sprintf("msg_%d", i), "acme", "order.paid", `{}`)
	}
	r := relay.New(e.pool, relay.Config{AllowPrivateNetworks: true, EndpointConcurrency: 2})
	if _, err := r.Dispatch(e.ctx); err != nil {
		t.Fatal(err)
	}
	// Two deliveries are in flight at another relay: the lease is the only trace.
	lease := func(until string) {
		t.Helper()
		if _, err := e.pool.Exec(e.ctx, `UPDATE webhook_deliveries SET leased_until = now() + $1::interval, next_attempt_at = now() + interval '1 hour'
			WHERE id IN (SELECT id FROM webhook_deliveries ORDER BY id LIMIT 2)`, until); err != nil {
			t.Fatal(err)
		}
	}
	lease("1 hour")
	if n, err := r.Deliver(e.ctx); err != nil || n != 0 {
		t.Fatalf("claimed %d (%v) with the endpoint full", n, err)
	}
	// That relay died: its leases ran out, so the slots are free again.
	lease("-1 second")
	if n, err := r.Deliver(e.ctx); err != nil || n != 1 {
		t.Fatalf("claimed %d (%v), want the one due delivery", n, err)
	}
}
