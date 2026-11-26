package schema_test

import (
	"context"
	"os"
	"testing"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/pgtest"
	"github.com/ianfoxdev/webhook-outbox/relay/internal/schema"
)

func TestMigrateTwice(t *testing.T) {
	conn := pgtest.Connect(t)
	ctx := context.Background()
	applied, err := schema.Migrate(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 2 || applied[0] != "0001_init.sql" || applied[1] != "0002_endpoint_limit.sql" {
		t.Fatalf("applied %v", applied)
	}
	applied, err = schema.Migrate(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("second run applied %v", applied)
	}
	var tables int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name LIKE 'webhook\_%'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 5 {
		t.Fatalf("%d webhook_ tables, want 5", tables)
	}
}

// The schema for teams that run their own migrations is the migrations put together.
func TestSchemaFileMatchesMigrations(t *testing.T) {
	want, err := os.ReadFile("../../../schema/postgresql.sql")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := schema.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	header, err := os.ReadFile("header.sql")
	if err != nil {
		t.Fatal(err)
	}
	got := string(header)
	for _, m := range migrations {
		got += "\n-- " + m.Name + "\n\n" + m.SQL
	}
	if got != string(want) {
		t.Fatal("schema/postgresql.sql differs from the migrations; run make schema")
	}
}
