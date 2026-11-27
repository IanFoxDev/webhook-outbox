// Package endpoint stores the URLs customers want their webhooks sent to.
package endpoint

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned for an id that is not in webhook_endpoints.
var ErrNotFound = errors.New("endpoint not found")

// Endpoint is one row of webhook_endpoints.
type Endpoint struct {
	ID       string
	Customer string
	URL      string
	Secret   string
	// EventTypes is nil when the endpoint takes every type.
	EventTypes     []string
	Description    string
	CreatedAt      time.Time
	DisabledAt     *time.Time
	DisabledReason string
	// PreviousSecretUntil is set while the secret before the last rotation is still
	// used to sign; nil when there is none or it has expired.
	PreviousSecretUntil *time.Time
}

// New is what a caller decides about a new endpoint; the id and secret are made here.
type New struct {
	Customer    string
	URL         string
	EventTypes  []string
	Description string
}

// Add validates and stores a new endpoint with a fresh id and secret.
func Add(ctx context.Context, conn *pgx.Conn, n New) (Endpoint, error) {
	if strings.TrimSpace(n.Customer) == "" {
		return Endpoint{}, errors.New("customer is empty")
	}
	if err := CheckURL(n.URL); err != nil {
		return Endpoint{}, err
	}
	for _, t := range n.EventTypes {
		if t == "" || strings.ContainsAny(t, " \t\n,") {
			return Endpoint{}, fmt.Errorf("event type %q: must be non-empty, without spaces or commas", t)
		}
	}
	e := Endpoint{
		ID:          "ep_" + token(16),
		Customer:    n.Customer,
		URL:         n.URL,
		Secret:      "whsec_" + base64.StdEncoding.EncodeToString(random(32)),
		EventTypes:  n.EventTypes,
		Description: n.Description,
	}
	err := conn.QueryRow(ctx, `INSERT INTO webhook_endpoints (id, customer, url, secret, event_types, description)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING created_at`,
		e.ID, e.Customer, e.URL, e.Secret, e.EventTypes, e.Description).Scan(&e.CreatedAt)
	if err != nil {
		return Endpoint{}, fmt.Errorf("insert endpoint: %w", err)
	}
	return e, nil
}

// CheckURL accepts an absolute http or https URL without credentials or a fragment.
// Whether the host may be reached is decided when the relay sends, not here.
func CheckURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("url: %w", err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("url %q: scheme must be http or https", raw)
	case u.Host == "":
		return fmt.Errorf("url %q: no host", raw)
	case u.User != nil:
		return fmt.Errorf("url %q: credentials in the URL end up in logs; use the signature instead", raw)
	case u.Fragment != "" || strings.Contains(raw, "#"):
		return fmt.Errorf("url %q: a fragment is never sent", raw)
	}
	return nil
}

const columns = `id, customer, url, secret, event_types, description, created_at, disabled_at, COALESCE(disabled_reason, ''),
	CASE WHEN previous_secret_until > now() THEN previous_secret_until END`

func scan(row pgx.Row) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.Customer, &e.URL, &e.Secret, &e.EventTypes, &e.Description, &e.CreatedAt, &e.DisabledAt, &e.DisabledReason,
		&e.PreviousSecretUntil)
	return e, err
}

// Get returns one endpoint.
func Get(ctx context.Context, conn *pgx.Conn, id string) (Endpoint, error) {
	e, err := scan(conn.QueryRow(ctx, `SELECT `+columns+` FROM webhook_endpoints WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return e, err
}

// List returns the endpoints of one customer, or of all when customer is empty, oldest
// first.
func List(ctx context.Context, conn *pgx.Conn, customer string) ([]Endpoint, error) {
	rows, err := conn.Query(ctx, `SELECT `+columns+` FROM webhook_endpoints
		WHERE $1 = '' OR customer = $1 ORDER BY created_at, id`, customer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Disable stops deliveries to the endpoint. Disabling a disabled endpoint keeps the
// first time and reason.
func Disable(ctx context.Context, conn *pgx.Conn, id, reason string) error {
	return update(ctx, conn, id, `UPDATE webhook_endpoints
		SET disabled_at = COALESCE(disabled_at, now()), disabled_reason = COALESCE(disabled_reason, $2)
		WHERE id = $1`, reason)
}

// Enable lets deliveries to the endpoint go out again.
func Enable(ctx context.Context, conn *pgx.Conn, id string) error {
	return update(ctx, conn, id, `UPDATE webhook_endpoints SET disabled_at = NULL, disabled_reason = NULL, failing_since = NULL WHERE id = $1`)
}

// Rotate gives the endpoint a new secret and returns it. The old secret keeps signing
// next to the new one for keep, so the customer can switch their receiver at any point
// in that window without rejecting a webhook. A keep of zero drops the old secret at
// once, for a secret that leaked. Rotating again within the window drops the oldest.
func Rotate(ctx context.Context, conn *pgx.Conn, id string, keep time.Duration) (Endpoint, error) {
	if keep < 0 {
		return Endpoint{}, errors.New("keep must not be negative")
	}
	secret := "whsec_" + base64.StdEncoding.EncodeToString(random(32))
	e, err := scan(conn.QueryRow(ctx, `UPDATE webhook_endpoints SET
			previous_secret = CASE WHEN $3::bigint > 0 THEN secret END,
			previous_secret_until = CASE WHEN $3::bigint > 0 THEN now() + $3::bigint * interval '1 microsecond' END,
			secret = $2
		WHERE id = $1
		RETURNING `+columns, id, secret, keep.Microseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return e, err
}

func update(ctx context.Context, conn *pgx.Conn, id, sql string, args ...any) error {
	tag, err := conn.Exec(ctx, sql, append([]any{id}, args...)...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return nil
}

func random(n int) []byte {
	b := make([]byte, n)
	// crypto/rand.Read never fails on supported platforms.
	_, _ = rand.Read(b)
	return b
}

var encoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func token(n int) string {
	return encoding.EncodeToString(random(n))
}
