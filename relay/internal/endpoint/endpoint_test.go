package endpoint_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/endpoint"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/schema"
	"github.com/jackc/pgx/v5"
)

func db(t *testing.T) *pgx.Conn {
	t.Helper()
	conn := pgtest.Connect(t)
	if _, err := schema.Migrate(context.Background(), conn); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestCheckURL(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://hooks.example.com/in":     true,
		"http://localhost:8080/webhooks":   true,
		"ftp://example.com":                false,
		"/relative":                        false,
		"https://":                         false,
		"https://user:pass@example.com/in": false,
		"https://example.com/in#x":         false,
		"https://example.com/in#":          false,
	} {
		if err := endpoint.CheckURL(raw); (err == nil) != ok {
			t.Errorf("%s: %v", raw, err)
		}
	}
}

func TestAddGetList(t *testing.T) {
	conn := db(t)
	ctx := context.Background()
	a, err := endpoint.Add(ctx, conn, endpoint.New{Customer: "acme", URL: "https://acme.example/hooks", EventTypes: []string{"order.paid"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a.ID, "ep_") || len(a.ID) != 3+26 {
		t.Fatalf("id %q", a.ID)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(a.Secret, "whsec_"))
	if !strings.HasPrefix(a.Secret, "whsec_") || err != nil || len(key) != 32 {
		t.Fatalf("secret %q: %v", a.Secret, err)
	}
	if _, err := endpoint.Add(ctx, conn, endpoint.New{Customer: "globex", URL: "https://globex.example/in"}); err != nil {
		t.Fatal(err)
	}
	got, err := endpoint.Get(ctx, conn, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Secret != a.Secret || len(got.EventTypes) != 1 || got.EventTypes[0] != "order.paid" || got.DisabledAt != nil {
		t.Fatalf("%+v", got)
	}
	all, err := endpoint.List(ctx, conn, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("%d endpoints, %v", len(all), err)
	}
	acme, err := endpoint.List(ctx, conn, "acme")
	if err != nil || len(acme) != 1 || acme[0].ID != a.ID {
		t.Fatalf("%+v, %v", acme, err)
	}
	other, err := endpoint.Get(ctx, conn, all[1].ID)
	if err != nil || other.EventTypes != nil {
		t.Fatalf("every type should be nil: %+v, %v", other, err)
	}
}

func TestAddRefuses(t *testing.T) {
	conn := db(t)
	for _, n := range []endpoint.New{
		{Customer: "", URL: "https://a.example"},
		{Customer: "acme", URL: "mailto:x@a.example"},
		{Customer: "acme", URL: "https://a.example", EventTypes: []string{"a,b"}},
		{Customer: "acme", URL: "https://a.example", EventTypes: []string{""}},
	} {
		if _, err := endpoint.Add(context.Background(), conn, n); err == nil {
			t.Errorf("%+v accepted", n)
		}
	}
}

func TestDisableEnable(t *testing.T) {
	conn := db(t)
	ctx := context.Background()
	e, err := endpoint.Add(ctx, conn, endpoint.New{Customer: "acme", URL: "https://a.example"})
	if err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Disable(ctx, conn, e.ID, "first"); err != nil {
		t.Fatal(err)
	}
	first, _ := endpoint.Get(ctx, conn, e.ID)
	if err := endpoint.Disable(ctx, conn, e.ID, "second"); err != nil {
		t.Fatal(err)
	}
	again, _ := endpoint.Get(ctx, conn, e.ID)
	if again.DisabledAt == nil || !again.DisabledAt.Equal(*first.DisabledAt) || again.DisabledReason != "first" {
		t.Fatalf("%+v", again)
	}
	// An endpoint the relay disabled for a failure streak starts a new one when it is
	// enabled, or the next failure would disable it again at once.
	if _, err := conn.Exec(ctx, `UPDATE webhook_endpoints SET failing_since = now() - interval '6 days' WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	if err := endpoint.Enable(ctx, conn, e.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := endpoint.Get(ctx, conn, e.ID); got.DisabledAt != nil || got.DisabledReason != "" {
		t.Fatalf("%+v", got)
	}
	var failing bool
	if err := conn.QueryRow(ctx, `SELECT failing_since IS NOT NULL FROM webhook_endpoints WHERE id = $1`, e.ID).Scan(&failing); err != nil || failing {
		t.Fatalf("failing_since kept after enable: %v", err)
	}
	for _, err := range []error{
		endpoint.Disable(ctx, conn, "ep_missing", ""),
		endpoint.Enable(ctx, conn, "ep_missing"),
		func() error { _, err := endpoint.Get(ctx, conn, "ep_missing"); return err }(),
	} {
		if !errors.Is(err, endpoint.ErrNotFound) {
			t.Errorf("got %v", err)
		}
	}
}

func TestRotate(t *testing.T) {
	conn := db(t)
	ctx := context.Background()
	e, err := endpoint.Add(ctx, conn, endpoint.New{Customer: "acme", URL: "https://a.example"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := endpoint.Rotate(ctx, conn, e.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if r.Secret == e.Secret || !strings.HasPrefix(r.Secret, "whsec_") || r.PreviousSecretUntil == nil ||
		time.Until(*r.PreviousSecretUntil) < 59*time.Minute || time.Until(*r.PreviousSecretUntil) > time.Hour {
		t.Fatalf("%+v", r)
	}
	var previous string
	if err := conn.QueryRow(ctx, `SELECT previous_secret FROM webhook_endpoints WHERE id = $1`, e.ID).Scan(&previous); err != nil || previous != e.Secret {
		t.Fatalf("previous secret %q, %v", previous, err)
	}
	// Rotating again keeps only the one before.
	again, err := endpoint.Rotate(ctx, conn, e.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT previous_secret FROM webhook_endpoints WHERE id = $1`, e.ID).Scan(&previous); err != nil || previous != r.Secret {
		t.Fatalf("previous secret after a second rotation %q, %v", previous, err)
	}
	// A leaked secret is dropped at once.
	now, err := endpoint.Rotate(ctx, conn, e.ID, 0)
	if err != nil || now.PreviousSecretUntil != nil || now.Secret == again.Secret {
		t.Fatalf("%+v, %v", now, err)
	}
	var kept bool
	if err := conn.QueryRow(ctx, `SELECT previous_secret IS NOT NULL FROM webhook_endpoints WHERE id = $1`, e.ID).Scan(&kept); err != nil || kept {
		t.Fatalf("old secret kept with keep 0: %v", err)
	}
	// An expired old secret is not reported.
	if _, err := endpoint.Rotate(ctx, conn, e.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `UPDATE webhook_endpoints SET previous_secret_until = now() - interval '1 second' WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := endpoint.Get(ctx, conn, e.ID); got.PreviousSecretUntil != nil {
		t.Fatalf("expired old secret reported: %+v", got)
	}
	if _, err := endpoint.Rotate(ctx, conn, "ep_missing", time.Hour); !errors.Is(err, endpoint.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := endpoint.Rotate(ctx, conn, e.ID, -time.Second); err == nil {
		t.Fatal("negative keep accepted")
	}
}
