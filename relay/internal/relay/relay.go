// Package relay turns queued messages into deliveries and sends them.
package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/netguard"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/signing"
)

// DefaultSchedule is the pause after each failed attempt: 8 attempts over about 28
// hours.
var DefaultSchedule = []time.Duration{
	5 * time.Second, 5 * time.Minute, 30 * time.Minute,
	2 * time.Hour, 5 * time.Hour, 10 * time.Hour, 10 * time.Hour,
}

// Config tunes the relay. Zero fields take the defaults of Defaults.
type Config struct {
	// PollInterval is the wait after a round that found less than a full batch.
	PollInterval time.Duration
	// BatchSize is how many messages are dispatched, and how many deliveries are
	// claimed, in one round.
	BatchSize int
	// Concurrency is how many requests are in flight at once.
	Concurrency int
	// EndpointConcurrency is how many requests are in flight to one endpoint, counted
	// over all relays sharing the database. A negative value turns the limit off.
	EndpointConcurrency int
	// Timeout is the limit for one request, from connecting to reading the response.
	Timeout time.Duration
	// Schedule is the pause after each failed attempt. A delivery that failed
	// len(Schedule)+1 times is marked failed.
	Schedule []time.Duration
	// ResponseLimit is how many bytes of a response body are kept in webhook_attempts.
	ResponseLimit int
	// DisableAfter disables an endpoint whose attempts have all failed for this long.
	// A negative value never disables. An endpoint that answers 410 Gone is disabled at
	// once either way.
	DisableAfter time.Duration
	// AllowPrivateNetworks lets the relay send to loopback, private and link-local
	// addresses. Leave it off when customers register their own URLs: see netguard.
	AllowPrivateNetworks bool
	// UserAgent is sent with every request.
	UserAgent string
	Logger    *slog.Logger
	// Metrics receives what this relay does. See package metrics.
	Metrics Metrics
}

// Metrics receives the events of one relay process.
type Metrics interface {
	// Dispatched counts messages turned into deliveries.
	Dispatched(messages int)
	// Attempt counts a recorded attempt. result is succeeded, retrying or failed (the
	// last attempt failed); status is the HTTP status, 0 when no response came.
	Attempt(result string, status int, d time.Duration)
	// EndpointDisabled counts an endpoint this relay disabled: reason is gone (410) or
	// failing (the failure streak ran past DisableAfter).
	EndpointDisabled(reason string)
}

type noMetrics struct{}

func (noMetrics) Dispatched(int)                     {}
func (noMetrics) Attempt(string, int, time.Duration) {}
func (noMetrics) EndpointDisabled(string)            {}

// Defaults fills the zero fields of c.
func (c Config) Defaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 16
	}
	if c.EndpointConcurrency == 0 {
		c.EndpointConcurrency = 4
	}
	if c.Timeout <= 0 {
		c.Timeout = 15 * time.Second
	}
	if c.Schedule == nil {
		c.Schedule = DefaultSchedule
	}
	if c.DisableAfter == 0 {
		c.DisableAfter = 5 * 24 * time.Hour
	}
	if c.ResponseLimit <= 0 {
		c.ResponseLimit = 4096
	}
	if c.UserAgent == "" {
		c.UserAgent = "webhook-outbox"
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Metrics == nil {
		c.Metrics = noMetrics{}
	}
	return c
}

// Relay sends the deliveries of one database. Several relays can share a database:
// they claim work with SKIP LOCKED and never take the same row at once.
type Relay struct {
	pool   *pgxpool.Pool
	cfg    Config
	client *http.Client
	// slots holds a token per request Run has in flight; freed wakes Run when one ends.
	slots chan struct{}
	freed chan struct{}
}

// New returns a relay for the database behind pool.
func New(pool *pgxpool.Pool, cfg Config) *Relay {
	cfg = cfg.Defaults()
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	if !cfg.AllowPrivateNetworks {
		dialer.Control = netguard.Control
	}
	return &Relay{
		pool:  pool,
		cfg:   cfg,
		slots: make(chan struct{}, cfg.Concurrency),
		freed: make(chan struct{}, 1),
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				// No proxy from the environment: the guard checks the address the relay
				// connects to, and through a proxy that is the proxy.
				Proxy:                 nil,
				DialContext:           dialer.DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   4,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: time.Second,
			},
			// A redirect is an answer, not a success: the endpoint should be updated.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Run works until ctx is cancelled. Requests in flight are finished, so a stop takes
// up to Timeout.
//
// Run does not wait for the requests it starts: it claims only as many deliveries as
// it has free request slots, and claims again when a slot frees up. A slow endpoint
// then holds its own requests and not the whole relay, and a claimed delivery is sent
// at once instead of waiting in the process while its lease runs out.
func (r *Relay) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	// A stop does not cut requests in flight: their results are recorded.
	work := context.WithoutCancel(ctx)
	for {
		busy, err := r.step(ctx, work, &wg)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.cfg.Logger.Error("round failed", "error", err)
		}
		if busy && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.freed:
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

// step dispatches a batch and starts requests for up to the free slots. It reports
// whether there is likely more to do right away.
func (r *Relay) step(ctx, work context.Context, wg *sync.WaitGroup) (bool, error) {
	dispatched, err := r.Dispatch(ctx)
	if err != nil {
		return false, err
	}
	free := min(cap(r.slots)-len(r.slots), r.cfg.BatchSize)
	if free == 0 {
		return dispatched == r.cfg.BatchSize, nil
	}
	claimed, err := r.claim(ctx, free)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	for _, d := range claimed {
		r.slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				<-r.slots
				select {
				case r.freed <- struct{}{}:
				default:
				}
				wg.Done()
			}()
			if err := r.record(work, d, r.send(work, d)); err != nil {
				r.cfg.Logger.Error("recording an attempt failed", "error", err)
			}
		}()
	}
	return dispatched == r.cfg.BatchSize || len(claimed) == free, nil
}

// Round dispatches one batch of messages and sends one batch of due deliveries, and
// returns when they are recorded. It reports whether either batch was full, so there
// is likely more to do. Run does the same without waiting for the requests.
func (r *Relay) Round(ctx context.Context) (bool, error) {
	dispatched, err := r.Dispatch(ctx)
	if err != nil {
		return false, err
	}
	sent, err := r.Deliver(ctx)
	if err != nil {
		return false, err
	}
	return dispatched == r.cfg.BatchSize || sent == r.cfg.BatchSize, nil
}

// Dispatch creates the deliveries of up to BatchSize new messages: one for each enabled
// endpoint of the message's customer that takes its type. It returns how many messages
// it took.
func (r *Relay) Dispatch(ctx context.Context) (int, error) {
	tag, err := r.pool.Exec(ctx, `
		WITH m AS (
			SELECT id, customer, type FROM webhook_messages
			WHERE dispatched_at IS NULL
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), d AS (
			INSERT INTO webhook_deliveries (message_id, endpoint_id)
			SELECT m.id, e.id FROM m
			JOIN webhook_endpoints e ON e.customer = m.customer
				AND e.disabled_at IS NULL
				AND (e.event_types IS NULL OR m.type = ANY (e.event_types))
			ON CONFLICT (message_id, endpoint_id) DO NOTHING
		)
		UPDATE webhook_messages w SET dispatched_at = now() FROM m WHERE w.id = m.id`, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("dispatch: %w", err)
	}
	if n := int(tag.RowsAffected()); n > 0 {
		r.cfg.Metrics.Dispatched(n)
	}
	return int(tag.RowsAffected()), nil
}

type delivery struct {
	id       int64
	endpoint string
	attempts int
	message  string
	payload  []byte
	url      string
	secret   string
}

// Deliver claims up to BatchSize due deliveries, sends them and records the results.
// It returns how many it claimed, after all of them are recorded.
func (r *Relay) Deliver(ctx context.Context) (int, error) {
	claimed, err := r.claim(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}

	// A stop does not cut requests in flight: their results are recorded.
	work := context.WithoutCancel(ctx)
	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, d := range claimed {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := r.record(work, d, r.send(work, d)); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return len(claimed), errors.Join(errs...)
}

// claimedRows is the end of a claim: the claimed rows joined with what a request needs.
const claimedRows = `
	SELECT u.id, u.endpoint_id, u.attempts, m.id, m.payload, e.url, e.secret
	FROM u
	JOIN webhook_messages m ON m.id = u.message_id
	JOIN webhook_endpoints e ON e.id = u.endpoint_id`

// claim takes due deliveries for this relay. Claiming moves next_attempt_at and
// leased_until past the end of the request, so the row is not due for anyone else
// while it is being sent, and counts as a request in flight to its endpoint. If this
// relay dies, the lease runs out and another relay sends it again: delivery is at
// least once.
func (r *Relay) claim(ctx context.Context, limit int) ([]delivery, error) {
	lease := (r.cfg.Timeout + 30*time.Second).Microseconds()
	if r.cfg.EndpointConcurrency < 0 {
		rows, err := r.pool.Query(ctx, `
			WITH c AS (
				SELECT d.id FROM webhook_deliveries d
				JOIN webhook_endpoints e ON e.id = d.endpoint_id
				WHERE d.status = 'pending' AND d.next_attempt_at <= now() AND e.disabled_at IS NULL
				ORDER BY d.next_attempt_at
				LIMIT $1
				FOR UPDATE OF d SKIP LOCKED
			), u AS (
				UPDATE webhook_deliveries d SET next_attempt_at = now() + $2 * interval '1 microsecond',
					leased_until = now() + $2 * interval '1 microsecond', updated_at = now()
				FROM c WHERE d.id = c.id
				RETURNING d.id, d.attempts, d.message_id, d.endpoint_id
			)`+claimedRows, limit, lease)
		if err != nil {
			return nil, err
		}
		return scanDeliveries(rows)
	}

	// With a limit per endpoint the claim takes two statements; ADR 0002 explains why.
	// The first locks the endpoints with due work, oldest due first, so no other relay
	// claims for them until this transaction ends. The second starts after the locks
	// are held, so its count of requests in flight sees every lease committed before.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT e.id FROM webhook_endpoints e
		CROSS JOIN LATERAL (
			SELECT d.next_attempt_at FROM webhook_deliveries d
			WHERE d.endpoint_id = e.id AND d.status = 'pending' AND d.next_attempt_at <= now()
			ORDER BY d.next_attempt_at
			LIMIT 1
		) due
		WHERE e.disabled_at IS NULL
		ORDER BY due.next_attempt_at
		LIMIT $1
		FOR NO KEY UPDATE OF e SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	endpoints, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(endpoints) == 0 {
		return nil, err
	}
	rows, err = tx.Query(ctx, `
		WITH f AS (
			SELECT e.id, $3 - (
				SELECT count(*) FROM webhook_deliveries x
				WHERE x.endpoint_id = e.id AND x.leased_until > now() AND x.status = 'pending'
			) AS free
			FROM unnest($1::text[]) AS e (id)
		), c AS (
			SELECT x.id FROM f CROSS JOIN LATERAL (
				SELECT d.id, d.next_attempt_at FROM webhook_deliveries d
				WHERE d.endpoint_id = f.id AND d.status = 'pending' AND d.next_attempt_at <= now()
				ORDER BY d.next_attempt_at
				LIMIT greatest(f.free, 0)
				FOR UPDATE SKIP LOCKED
			) x
			ORDER BY x.next_attempt_at
			LIMIT $4
		), u AS (
			UPDATE webhook_deliveries d SET next_attempt_at = now() + $2 * interval '1 microsecond',
				leased_until = now() + $2 * interval '1 microsecond', updated_at = now()
			FROM c WHERE d.id = c.id
			RETURNING d.id, d.attempts, d.message_id, d.endpoint_id
		)`+claimedRows, endpoints, lease, r.cfg.EndpointConcurrency, limit)
	if err != nil {
		return nil, err
	}
	out, err := scanDeliveries(rows)
	if err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}

func scanDeliveries(rows pgx.Rows) ([]delivery, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (delivery, error) {
		var d delivery
		var payload string
		err := row.Scan(&d.id, &d.endpoint, &d.attempts, &d.message, &payload, &d.url, &d.secret)
		d.payload = []byte(payload)
		return d, err
	})
}

// result is what one attempt got.
type result struct {
	started    time.Time
	duration   time.Duration
	status     int // 0 when there was no response
	err        string
	response   string
	retryAfter string
}

func (r result) ok() bool { return r.status >= 200 && r.status <= 299 }

// gone is the receiver asking to stop: the endpoint is disabled at once.
func (r result) gone() bool { return r.status == http.StatusGone }

func (r *Relay) send(ctx context.Context, d delivery) (res result) {
	res.started = time.Now()
	defer func() { res.duration = time.Since(res.started) }()
	key, err := signing.Key(d.secret)
	if err != nil {
		res.err = err.Error()
		return res
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(d.payload))
	if err != nil {
		res.err = err.Error()
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", r.cfg.UserAgent)
	signing.Sign(req.Header, key, d.message, res.started, d.payload)
	resp, err := r.client.Do(req)
	if err != nil {
		res.err = err.Error()
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	res.status = resp.StatusCode
	if res.status == http.StatusTooManyRequests || res.status == http.StatusServiceUnavailable {
		res.retryAfter = resp.Header.Get("Retry-After")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(r.cfg.ResponseLimit)))
	if err != nil && res.err == "" {
		res.err = "reading the response: " + err.Error()
	}
	res.response = text(body)
	return res
}

// record stores the attempt and moves the delivery on: succeeded, due again after the
// next pause, or failed after the last attempt. It also keeps the endpoint's health:
// a success clears failing_since, a failure sets it, and a 410 or a failure streak
// longer than DisableAfter disables the endpoint. Its pending deliveries then wait
// until it is enabled again.
func (r *Relay) record(ctx context.Context, d delivery, res result) error {
	attempts := d.attempts + 1
	status, pause := "pending", time.Duration(0)
	switch {
	case res.ok():
		status = "succeeded"
	case attempts > len(r.cfg.Schedule):
		status = "failed"
	default:
		pause = jitter(r.cfg.Schedule[attempts-1])
		if res.retryAfter != "" {
			pause = max(pause, retryAfterPause(res.retryAfter, time.Now(), slices.Max(r.cfg.Schedule)))
		}
	}
	var code *int
	if res.status != 0 {
		code = &res.status
	}
	var errText *string
	if res.retryAfter != "" {
		if res.err != "" {
			res.err += "\n"
		}
		res.err += "Retry-After: " + res.retryAfter[:min(len(res.retryAfter), r.cfg.ResponseLimit)]
	}
	if res.err != "" {
		e := text([]byte(res.err))
		errText = &e
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("record delivery %d: %w", d.id, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO webhook_attempts (delivery_id, started_at, duration_ms, status_code, error, response)
		VALUES ($1, $2, $3, $4, $5, $6)`, d.id, res.started, res.duration.Milliseconds(), code, errText, res.response); err != nil {
		return fmt.Errorf("record delivery %d: %w", d.id, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE webhook_deliveries
		SET attempts = $2, status = $3, next_attempt_at = now() + $4 * interval '1 microsecond', leased_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'pending'`, d.id, attempts, status, pause.Microseconds()); err != nil {
		return fmt.Errorf("record delivery %d: %w", d.id, err)
	}
	disabled, err := r.health(ctx, tx, d.endpoint, res)
	if err != nil {
		return fmt.Errorf("record delivery %d: %w", d.id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("record delivery %d: %w", d.id, err)
	}
	result := map[string]string{"succeeded": "succeeded", "pending": "retrying", "failed": "failed"}[status]
	r.cfg.Metrics.Attempt(result, res.status, res.duration)
	if disabled != "" {
		r.cfg.Logger.Warn("endpoint disabled", "endpoint", d.endpoint, "reason", disabled)
		if res.gone() {
			r.cfg.Metrics.EndpointDisabled("gone")
		} else {
			r.cfg.Metrics.EndpointDisabled("failing")
		}
	}
	r.cfg.Logger.Debug("attempt", "delivery", d.id, "message", d.message, "status", res.status, "error", res.err, "result", status)
	return nil
}

func retryAfterPause(header string, now time.Time, limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	header = strings.TrimSpace(header)
	if header != "" && strings.Trim(header, "0123456789") == "" {
		seconds, err := strconv.ParseUint(header, 10, 64)
		if err != nil || seconds > uint64(limit/time.Second) {
			return limit
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(header); err == nil {
		return min(max(0, date.Sub(now)), limit)
	}
	return 0
}

// health updates the endpoint after an attempt and returns the reason when the
// attempt disabled it. Rows are only written when something changes, so a healthy
// endpoint is not updated on every delivery.
func (r *Relay) health(ctx context.Context, tx pgx.Tx, endpoint string, res result) (string, error) {
	if res.ok() {
		_, err := tx.Exec(ctx, `UPDATE webhook_endpoints SET failing_since = NULL
			WHERE id = $1 AND failing_since IS NOT NULL`, endpoint)
		return "", err
	}
	after := int64(-1)
	if r.cfg.DisableAfter > 0 {
		after = r.cfg.DisableAfter.Microseconds()
	}
	var reason *string
	err := tx.QueryRow(ctx, `
		WITH e AS (
			SELECT id, failing_since,
				disabled_at IS NULL AND ($2 OR ($3::bigint >= 0 AND failing_since <= now() - $3::bigint * interval '1 microsecond')) AS stop
			FROM webhook_endpoints WHERE id = $1
		)
		UPDATE webhook_endpoints w SET
			failing_since = COALESCE(e.failing_since, now()),
			disabled_at = CASE WHEN e.stop THEN now() ELSE w.disabled_at END,
			disabled_reason = CASE WHEN e.stop THEN
				CASE WHEN $2 THEN 'the endpoint answered 410 Gone'
				ELSE 'no successful attempt since ' || to_char(e.failing_since AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END
				ELSE w.disabled_reason END
		FROM e
		WHERE w.id = e.id AND (e.failing_since IS NULL OR e.stop)
		RETURNING CASE WHEN e.stop THEN w.disabled_reason END`, endpoint, res.gone(), after).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && reason == nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return *reason, nil
}

// jitter spreads a pause by up to a tenth either way, so endpoints that failed together
// are not retried together.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	spread := int64(d) / 10
	return d + time.Duration(rand.Int64N(2*spread+1)-spread)
}

// text makes bytes storable in a PostgreSQL text column.
func text(b []byte) string {
	return strings.ReplaceAll(strings.ToValidUTF8(string(b), "?"), "\x00", "")
}
