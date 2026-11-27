package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/pgtest"
)

// cli runs a command with the given environment and returns its exit code and output.
func cli(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), args, func(k string) string { return env[k] }, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestVersion(t *testing.T) {
	code, out, _ := cli(t, nil, "version")
	if code != 0 || out != "dev\n" {
		t.Fatalf("exit %d, %q", code, out)
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"nope"}} {
		if code, _, errOut := cli(t, nil, args...); code != 2 || errOut == "" {
			t.Fatalf("%v: exit %d", args, code)
		}
	}
}

func TestMigrateNeedsDatabaseURL(t *testing.T) {
	code, _, errOut := cli(t, nil, "migrate")
	if code != 2 || errOut != "DATABASE_URL is not set\n" {
		t.Fatalf("exit %d, %q", code, errOut)
	}
}

func TestMigrate(t *testing.T) {
	env := map[string]string{"DATABASE_URL": pgtest.URL(t)}
	if code, out, errOut := cli(t, env, "migrate"); code != 0 || out != "applied 0001_init.sql\napplied 0002_endpoint_limit.sql\napplied 0003_secret_rotation.sql\n" {
		t.Fatalf("exit %d, %q, %q", code, out, errOut)
	}
	if code, out, _ := cli(t, env, "migrate"); code != 0 || out != "up to date\n" {
		t.Fatalf("exit %d, %q", code, out)
	}
}

func TestEndpoints(t *testing.T) {
	env := map[string]string{"DATABASE_URL": pgtest.URL(t)}
	if code, _, errOut := cli(t, env, "migrate"); code != 0 {
		t.Fatal(errOut)
	}
	code, out, errOut := cli(t, env, "endpoints", "add", "-customer", "acme", "-url", "https://acme.example/hooks", "-types", "order.paid,order.refunded")
	if code != 0 {
		t.Fatalf("add: exit %d, %s", code, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "id ep_") || !strings.HasPrefix(lines[1], "secret whsec_") {
		t.Fatalf("add printed %q", out)
	}
	id, secret := strings.TrimPrefix(lines[0], "id "), strings.TrimPrefix(lines[1], "secret ")

	if code, out, _ := cli(t, env, "endpoints", "secret", id); code != 0 || out != secret+"\n" {
		t.Fatalf("secret: exit %d, %q", code, out)
	}
	if code, _, _ := cli(t, env, "endpoints", "disable", id, "-reason", "customer asked"); code != 0 {
		t.Fatal("disable failed")
	}
	code, out, _ = cli(t, env, "endpoints", "list", "-customer", "acme")
	if code != 0 || !strings.Contains(out, id) || !strings.Contains(out, "order.paid,order.refunded") ||
		!strings.Contains(out, "disabled: customer asked") || strings.Contains(out, secret) {
		t.Fatalf("list: exit %d\n%s", code, out)
	}
	if code, _, _ := cli(t, env, "endpoints", "enable", id); code != 0 {
		t.Fatal("enable failed")
	}
	if _, out, _ := cli(t, env, "endpoints", "list"); !strings.Contains(out, "enabled") {
		t.Fatalf("list after enable:\n%s", out)
	}
	if code, _, errOut := cli(t, env, "endpoints", "enable", "ep_missing"); code != 1 || !strings.Contains(errOut, "endpoint not found") {
		t.Fatalf("missing: exit %d, %q", code, errOut)
	}
	code, out, errOut = cli(t, env, "endpoints", "rotate", id, "-keep", "2d")
	lines = strings.Split(strings.TrimSpace(out), "\n")
	if code != 0 || len(lines) != 2 || !strings.HasPrefix(lines[0], "secret whsec_") || lines[0] == "secret "+secret ||
		!strings.HasPrefix(lines[1], "old secret signs until ") {
		t.Fatalf("rotate: exit %d, %q, %q", code, out, errOut)
	}
	if _, out, _ := cli(t, env, "endpoints", "secret", id); out != strings.TrimPrefix(lines[0], "secret ")+"\n" {
		t.Fatalf("secret after rotate: %q", out)
	}
	if _, out, _ := cli(t, env, "endpoints", "list"); !strings.Contains(out, "enabled, old secret until ") {
		t.Fatalf("list after rotate:\n%s", out)
	}
	if code, out, _ := cli(t, env, "endpoints", "rotate", id, "-keep", "0"); code != 0 || strings.Contains(out, "old secret") {
		t.Fatalf("rotate -keep 0: exit %d, %q", code, out)
	}
	if code, _, errOut := cli(t, env, "endpoints", "rotate", "ep_missing"); code != 1 || !strings.Contains(errOut, "endpoint not found") {
		t.Fatalf("rotate missing: exit %d, %q", code, errOut)
	}
	if code, _, _ := cli(t, env, "endpoints", "rotate", id, "-keep", "soon"); code != 2 {
		t.Fatalf("rotate -keep soon: exit %d", code)
	}
	if code, _, errOut := cli(t, env, "endpoints", "add", "-customer", "acme", "-url", "ftp://x"); code != 1 || errOut == "" {
		t.Fatalf("bad url: exit %d", code)
	}
	for _, args := range [][]string{{"endpoints"}, {"endpoints", "nope"}, {"endpoints", "secret"}, {"endpoints", "rotate"}, {"endpoints", "add", "extra"}} {
		if code, _, _ := cli(t, env, args...); code != 2 {
			t.Errorf("%v: exit %d", args, code)
		}
	}
}
