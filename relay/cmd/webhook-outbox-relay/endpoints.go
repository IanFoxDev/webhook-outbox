package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ianfoxdev/webhook-outbox/relay/internal/endpoint"
)

const endpointsUsage = `usage: webhook-outbox-relay endpoints <command>

  add -customer C -url U [-types a,b] [-description D]
             add an endpoint; prints its id and signing secret
  list [-customer C]
             list endpoints, without secrets
  secret ID  print the signing secret of an endpoint
  rotate ID [-keep 24h]
             give an endpoint a new secret; the old one keeps signing next to it
             for -keep (a duration, or days such as 3d; 0 drops it at once)
  disable ID [-reason R]
             stop deliveries to an endpoint
  enable ID  let deliveries go out again
`

func endpoints(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, endpointsUsage)
		return 2
	}
	var cmd func(*pgx.Conn) error
	switch args[0] {
	case "add":
		fs := flagSet("add", stderr)
		customer := fs.String("customer", "", "the application's id for the customer")
		url := fs.String("url", "", "where to send")
		types := fs.String("types", "", "comma-separated event types; empty means every type")
		description := fs.String("description", "", "a note for people")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
			return 2
		}
		var list []string
		if *types != "" {
			list = strings.Split(*types, ",")
		}
		cmd = func(conn *pgx.Conn) error {
			e, err := endpoint.Add(ctx, conn, endpoint.New{Customer: *customer, URL: *url, EventTypes: list, Description: *description})
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "id", e.ID)
			fmt.Fprintln(stdout, "secret", e.Secret)
			return nil
		}
	case "list":
		fs := flagSet("list", stderr)
		customer := fs.String("customer", "", "only this customer's endpoints")
		if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
			return 2
		}
		cmd = func(conn *pgx.Conn) error {
			list, err := endpoint.List(ctx, conn, *customer)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tCUSTOMER\tURL\tTYPES\tSTATUS")
			for _, e := range list {
				types := "*"
				if e.EventTypes != nil {
					types = strings.Join(e.EventTypes, ",")
				}
				status := "enabled"
				if e.DisabledAt != nil {
					status = "disabled"
					if e.DisabledReason != "" {
						status += ": " + e.DisabledReason
					}
				}
				if e.PreviousSecretUntil != nil {
					status += ", old secret until " + e.PreviousSecretUntil.UTC().Format(time.RFC3339)
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", e.ID, e.Customer, e.URL, types, status)
			}
			return w.Flush()
		}
	case "secret", "enable":
		if len(args) != 2 {
			fmt.Fprint(stderr, endpointsUsage)
			return 2
		}
		id := args[1]
		cmd = func(conn *pgx.Conn) error {
			if args[0] == "enable" {
				return endpoint.Enable(ctx, conn, id)
			}
			e, err := endpoint.Get(ctx, conn, id)
			if err == nil {
				fmt.Fprintln(stdout, e.Secret)
			}
			return err
		}
	case "rotate":
		if len(args) < 2 {
			fmt.Fprint(stderr, endpointsUsage)
			return 2
		}
		id := args[1]
		fs := flagSet("rotate", stderr)
		keepText := fs.String("keep", "24h", "how long the old secret keeps signing")
		if fs.Parse(args[2:]) != nil || fs.NArg() != 0 {
			return 2
		}
		keep, ok := days(*keepText)
		if *keepText == "0" {
			keep, ok = 0, true
		}
		if !ok {
			fmt.Fprintf(stderr, "endpoints rotate: -keep %q is not a duration such as 24h, days such as 3d, or 0\n", *keepText)
			return 2
		}
		cmd = func(conn *pgx.Conn) error {
			e, err := endpoint.Rotate(ctx, conn, id, keep)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, "secret", e.Secret)
			if e.PreviousSecretUntil != nil {
				fmt.Fprintln(stdout, "old secret signs until", e.PreviousSecretUntil.UTC().Format(time.RFC3339))
			}
			return nil
		}
	case "disable":
		if len(args) < 2 {
			fmt.Fprint(stderr, endpointsUsage)
			return 2
		}
		id := args[1]
		fs := flagSet("disable", stderr)
		reason := fs.String("reason", "", "why, for whoever enables it again")
		if fs.Parse(args[2:]) != nil || fs.NArg() != 0 {
			return 2
		}
		cmd = func(conn *pgx.Conn) error { return endpoint.Disable(ctx, conn, id, *reason) }
	default:
		fmt.Fprint(stderr, endpointsUsage)
		return 2
	}
	return withDB(ctx, getenv, stderr, cmd)
}

func flagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("endpoints "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}
