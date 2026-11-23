package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fragshq/mneme"
	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
	"github.com/fragshq/mneme/store/memstore"
	"github.com/fragshq/mneme/store/storetest"

	pgstore "github.com/fragshq/mneme-plugins/mneme-postgres-storage"
)

// Set MNEME_PG_DSN (a postgres:// URL) to run these tests, e.g.
//
//	docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=pw postgres:17
//	MNEME_PG_DSN=postgres://postgres:pw@localhost:55432/postgres?sslmode=disable go test ./...
func dsn(t *testing.T) string {
	d := os.Getenv("MNEME_PG_DSN")
	if d == "" {
		t.Skip("MNEME_PG_DSN not set")
	}
	return d
}

var seq atomic.Int64

// isolated opens a pool whose search_path is a fresh, empty schema.
func isolated(t *testing.T, base string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("mneme_test_%d_%d", time.Now().UnixNano(), seq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE")
		admin.Close()
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	db, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

// suiteTenant is the tenant of every operation in the tenancy variants,
// whose tests run shared code with a bare context (the conformance suite,
// plan checks). It spans two columns, suiteColumns.
var (
	suiteTenant  = []string{"suite", "alice"}
	suiteColumns = []string{"org_id", "user_id"}
)

func fixedTenant(context.Context) []string { return suiteTenant }

var variants = []struct {
	name string
	opts pgstore.Options
}{
	{"TablePerType", pgstore.Options{Layout: pgstore.TablePerType}},
	{"SharedTable", pgstore.Options{Layout: pgstore.SharedTable}},
	{"TablePerTypeTenantGIN", pgstore.Options{Layout: pgstore.TablePerType, TenantColumns: suiteColumns, Tenant: fixedTenant, IndexAllFields: true}},
	{"SharedTableTenantGIN", pgstore.Options{Layout: pgstore.SharedTable, TenantColumns: suiteColumns, Tenant: fixedTenant, IndexAllFields: true}},
}

func open(t *testing.T, db pgstore.DB, opts pgstore.Options) *pgstore.Store {
	t.Helper()
	s, err := pgstore.New(context.Background(), db, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func engine(t *testing.T, s *pgstore.Store) *mneme.Engine {
	t.Helper()
	return storetest.Engine(t, s)
}

func normalized(t *testing.T, q query.Query) query.Query {
	t.Helper()
	n, err := q.Normalize(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConformance(t *testing.T) {
	base := dsn(t)
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			storetest.Run(t, func(t *testing.T) store.Store { return open(t, isolated(t, base), v.opts) })
		})
	}
}

func TestOptionsValidation(t *testing.T) {
	db := isolated(t, dsn(t))
	for _, cols := range [][]string{{"org id"}, {"id"}, {"org", "ORG"}, {"Data"}} {
		if _, err := pgstore.New(context.Background(), db, pgstore.Options{Layout: pgstore.SharedTable, TenantColumns: cols}); err == nil {
			t.Fatalf("tenant columns %q must be rejected", cols)
		}
	}
	if _, err := pgstore.New(context.Background(), db, pgstore.Options{}); err == nil {
		t.Fatal("a missing layout must be rejected")
	}
	if _, err := pgstore.New(context.Background(), db, pgstore.Options{Layout: pgstore.SharedTable, Prefix: `x"; drop`}); err == nil {
		t.Fatal("a non-identifier prefix must be rejected")
	}
}

// TestTenancy runs the tenancy conformance suite in both layouts.
func TestTenancy(t *testing.T) {
	base := dsn(t)
	for _, layout := range []struct {
		name   string
		layout pgstore.Layout
	}{{"TablePerType", pgstore.TablePerType}, {"SharedTable", pgstore.SharedTable}} {
		t.Run(layout.name, func(t *testing.T) {
			storetest.RunTenancy(t, func(t *testing.T, tenant store.TenantFunc, fields []string) store.Store {
				return open(t, isolated(t, base), pgstore.Options{Layout: layout.layout, TenantColumns: fields, Tenant: tenant})
			})
		})
	}
}

// With TablePerType, defining a record type creates its table up front, and
// a definition the store rejects leaves nothing behind.
func TestDefineCreatesTable(t *testing.T) {
	db := isolated(t, dsn(t))
	s := open(t, db, pgstore.Options{Layout: pgstore.TablePerType, Prefix: "app_"})
	e := engine(t, s)
	ctx := context.Background()
	if _, err := e.Define(ctx, schema.Definition{Name: "risk-item", Schema: &schema.JSONSchema{Type: "object"}}); err != nil {
		t.Fatal(err)
	}
	if got := s.RecordTable("risk-item"); got != "app_rec_risk-item" {
		t.Fatalf("RecordTable: %s", got)
	}
	n := 0
	tables := `SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`
	if err := db.QueryRow(ctx, tables, "app_rec_risk-item").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("table not created by Define")
	}
	if _, err := e.Define(ctx, schema.Definition{Name: "bad", Indexes: []string{"a..b"}, Schema: &schema.JSONSchema{Type: "object"}}); !errors.Is(err, mneme.ErrInvalid) {
		t.Fatalf("invalid index field: want ErrInvalid, got %v", err)
	}
	if _, err := e.GetRecordType(ctx, "bad", 0); !errors.Is(err, mneme.ErrNotFound) {
		t.Fatalf("a rejected definition must not be stored: %v", err)
	}
	if err := db.QueryRow(ctx, tables, "app_rec_bad").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("a rejected definition must not leave its table behind")
	}
}

// plan returns the EXPLAIN output of the SQL ListRecords runs for q, with
// sequential scans disabled so the result doesn't depend on table size.
func plan(t *testing.T, db *pgxpool.Pool, s *pgstore.Store, q query.Query) string {
	t.Helper()
	ctx := context.Background()
	sql, args, err := s.ListSQL(ctx, "risk", normalized(t, q))
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "EXPLAIN "+sql, args...)
		if err != nil {
			return err
		}
		lines, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

// Declared indexes must actually serve filters, and the default sort must
// page through the (created_at, id) index.
func TestIndexesServeQueries(t *testing.T) {
	base := dsn(t)
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			db := isolated(t, base)
			s := open(t, db, v.opts)
			e := engine(t, s)
			ctx := context.Background()
			if _, err := e.Define(ctx, schema.Definition{Name: "risk", Indexes: []string{"owner.id", "/severity", "/we?ird'key"},
				Schema: &schema.JSONSchema{Type: "object"}}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				body := fmt.Sprintf(`{"owner":{"id":"u%d"},"severity":%d,"we?ird'key":"k%d"}`, i%4, i%5, i)
				if _, err := e.Insert(ctx, "risk", json.RawMessage(body)); err != nil {
					t.Fatal(err)
				}
			}
			db.Exec(ctx, "ANALYZE")
			shared := v.opts.Layout == pgstore.SharedTable
			t.Run("field indexes only with TablePerType", func(t *testing.T) {
				n := 0
				err := db.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema()
					AND tablename = $1 AND indexdef LIKE '%data #>%'`, s.RecordTable("risk")).Scan(&n)
				if err != nil {
					t.Fatal(err)
				}
				if want := map[bool]int{false: 6, true: 0}[shared]; n != want {
					t.Fatalf("want %d expression indexes on data, got %d", want, n)
				}
			})
			for _, c := range []struct {
				name string
				f    query.Filter
				want int
			}{
				{"eq", query.Eq("owner.id", "u1"), 5},
				{"in", query.In("owner.id", "u1", "u2"), 10},
				{"gt number", query.Gt("/severity", 2), 8},
				{"lte number", query.Lte("/severity", 1), 8},
				{"gte string", query.Gte("/owner/id", "u2"), 10},
				{"prefix", query.Prefix("/we?ird'key", "k1"), 11},
			} {
				t.Run(c.name, func(t *testing.T) {
					p, err := e.List(ctx, "risk", query.Query{Filter: &c.f, Limit: 100})
					if err != nil {
						t.Fatal(err)
					}
					if len(p.Items) != c.want {
						t.Fatalf("want %d items, got %d", c.want, len(p.Items))
					}
					// Sorting by $id keeps the created_at index out of the
					// running, so the plan must pick a field index.
					got := plan(t, db, s, query.Query{Filter: &c.f, Sort: []query.SortField{{Field: "$id"}}, Limit: 100})
					if shared {
						if strings.Contains(got, "_f") {
							t.Fatalf("SharedTable must have no field indexes:\n%s", got)
						}
						return
					}
					if !strings.Contains(got, "_f") {
						t.Fatalf("filter does not use a declared index:\n%s", got)
					}
				})
			}
			t.Run("default sort pages through an index", func(t *testing.T) {
				first, err := e.List(ctx, "risk", query.Query{Limit: 5})
				if err != nil {
					t.Fatal(err)
				}
				got := plan(t, db, s, query.Query{Limit: 5, Cursor: first.NextCursor})
				if !strings.Contains(got, "_created") || strings.Contains(got, "Sort ") {
					t.Fatalf("default keyset page should be an ordered scan of the created_at index:\n%s", got)
				}
			})
			t.Run("labels contains uses the GIN index", func(t *testing.T) {
				// A selective label on a table big enough for the planner
				// to prefer the GIN index over scanning a tenant's rows.
				tenantCols, tenant := []string{"tenant"}, []any{""}
				if v.opts.Tenant != nil {
					tenantCols, tenant = suiteColumns, []any{suiteTenant[0], suiteTenant[1]}
				}
				ph := []string{}
				for i := range tenant {
					ph = append(ph, fmt.Sprintf("$%d", i+1))
				}
				cols, vals := strings.Join(tenantCols, ", ")+", id", strings.Join(ph, ", ")+", 'bulk-' || i"
				if v.opts.Layout == pgstore.SharedTable {
					cols, vals = strings.Join(tenantCols, ", ")+", record_type, id", strings.Join(ph, ", ")+", 'risk', 'bulk-' || i"
				}
				_, err := db.Exec(ctx, fmt.Sprintf(`INSERT INTO %q (%s, schema_version, created_at, updated_at, labels, data)
					SELECT %s, 1, now(), now(), CASE WHEN i %% 1000 = 0 THEN '["urgent"]' ELSE '["routine"]' END::jsonb, '{}'
					FROM generate_series(1, 5000) AS i`, s.RecordTable("risk"), cols, vals), tenant...)
				if err != nil {
					t.Fatal(err)
				}
				db.Exec(ctx, "ANALYZE")
				f := query.Contains("$labels", "urgent")
				if got := plan(t, db, s, query.Query{Filter: &f, Sort: []query.SortField{{Field: "/severity"}}}); !strings.Contains(got, "_labels") {
					t.Fatalf("labels filter does not use the labels index:\n%s", got)
				}
			})
		})
	}
}

// Every filter, alone and under NOT, must select exactly what memstore (the
// reference) selects, with and without the GIN pre-filters.
func TestFiltersMatchReference(t *testing.T) {
	base := dsn(t)
	bodies := []string{
		`{"a":1,"s":"apple","t":["x","y"],"o":{"k":"v"},"n":null}`,
		`{"a":1.0,"s":"banana","t":["y"],"o":{"k":"w","z":1}}`,
		`{"a":"1","s":"Apple","t":"xylophone","o":[{"k":"v"}]}`,
		`{"a":[1],"s":"","t":[["x"]],"o":{"k":"v"},"n":false}`,
		`{"a":true,"t":[],"o":{}}`,
		`{"s":"applesauce","t":[1,2],"a":-3.5e2}`,
		`{"a":[],"t":[{"k":"v"},"x"],"s":"été"}`,
		`{}`,
	}
	filters := []query.Filter{
		query.Eq("/a", 1), query.Eq("/a", "1"), query.Eq("/a", []any{1}), query.Eq("/o", map[string]any{"k": "v"}),
		query.Eq("/n", nil), query.Eq("o.k", "v"), query.Eq("/o/0/k", "v"), query.Eq("/t", []any{}),
		query.Ne("/a", 1), query.In("/a", 1, "1", true), query.In("o.k", "v", "w"),
		query.Gt("/a", 0), query.Gte("/a", 1), query.Lt("/a", 1), query.Lte("/a", -350),
		query.Gt("/s", "apple"), query.Lt("/s", "b"), query.Gte("/s", ""), query.Gt("/s", "z"), query.Gt("/a", "0"),
		query.Prefix("/s", "app"), query.Prefix("/s", ""), query.Prefix("/a", "1"),
		query.Contains("/t", "x"), query.Contains("/t", "y"), query.Contains("/t", 1), query.Contains("/t", []any{"x"}),
		query.Contains("/t", map[string]any{"k": "v"}), query.Contains("/s", "pp"), query.Contains("/o", "k"),
		query.Exists("/n"), query.Exists("/o/k"),
		query.Contains("$labels", "l1"), query.Eq("$labels", []any{}), query.Ne("$labels", []any{"l1"}),
		query.Contains("$id", "-"), query.Prefix("$id", "x"), query.Gt("$id", "x-3"), query.In("$id", "x-1", 2),
		query.Ne("$id", 3), query.Eq("$id", 3), query.Gt("$version", 0), query.Eq("$version", 1),
		query.Eq("$createdAt", "2026-01-01T00:00:02Z"), query.Ne("$createdAt", "2026-01-01T02:00:02+02:00"),
		query.In("$createdAt", "2026-01-01T00:00:01Z", "2026-01-01T00:00:03.000Z"), query.Lt("$createdAt", "2026-01-01T00:00:03Z"),
		query.Gte("$updatedAt", "2026-01-01T00:00:04Z"), query.Lte("$updatedAt", "2026-01-01T00:00:04.5Z"),
		query.Contains("$createdAt", "2026-01-01T00:00:02Z"),
	}
	for _, v := range variants[:2] {
		opts := v.opts
		opts.IndexAllFields = v.opts.Layout == pgstore.SharedTable
		pg := open(t, isolated(t, base), opts)
		mem := memstore.New()
		ctx := context.Background()
		for i, body := range bodies {
			at := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)
			labels := []string{}
			if i%2 == 0 {
				labels = []string{"l1"}
			}
			if i%3 == 0 { // byte order: "Zz" < "l1" < "é"
				labels = append(append([]string{"Zz"}, labels...), "é")
			}
			r := store.Record{RecordType: "doc", ID: fmt.Sprintf("x-%d", i), SchemaVersion: 1 + i%2, Data: json.RawMessage(body),
				CreatedAt: at, UpdatedAt: at, Labels: labels}
			if err := pg.InsertRecord(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err := mem.InsertRecord(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		for _, f := range filters {
			for _, g := range []query.Filter{f, query.Not(f), query.Not(query.Not(f)), query.Or(f, query.Not(f)), query.And(f, query.Exists("/a"))} {
				q, err := query.Query{Filter: &g}.Normalize(100, 100)
				if err != nil {
					t.Fatal(err)
				}
				want, err := mem.ListRecords(ctx, "doc", q)
				if err != nil {
					t.Fatal(err)
				}
				got, err := pg.ListRecords(ctx, "doc", q)
				raw, _ := json.Marshal(g)
				if err != nil {
					t.Fatalf("%s %s: %v", v.name, raw, err)
				}
				if storetest.IDs(got.Items) != storetest.IDs(want.Items) {
					t.Errorf("%s %s: want [%s], got [%s]", v.name, raw, storetest.IDs(want.Items), storetest.IDs(got.Items))
				}
				wantLabels, err := mem.ListLabels(ctx, "doc", q.Filter)
				if err != nil {
					t.Fatal(err)
				}
				gotLabels, err := pg.ListLabels(ctx, "doc", q.Filter)
				if err != nil {
					t.Fatalf("%s %s: ListLabels: %v", v.name, raw, err)
				}
				if w, g := fmt.Sprint(wantLabels), fmt.Sprint(gotLabels); w != g {
					t.Errorf("%s %s: labels: want %s, got %s", v.name, raw, w, g)
				}
				wantCount, err := mem.CountRecords(ctx, "doc", q.Filter)
				if err != nil {
					t.Fatal(err)
				}
				gotCount, err := pg.CountRecords(ctx, "doc", q.Filter)
				if err != nil {
					t.Fatalf("%s %s: CountRecords: %v", v.name, raw, err)
				}
				if wantCount != gotCount {
					t.Errorf("%s %s: count: want %d, got %d", v.name, raw, wantCount, gotCount)
				}
			}
		}
	}
}

func TestHostileFieldNamesAndCursors(t *testing.T) {
	base := dsn(t)
	s := open(t, isolated(t, base), pgstore.Options{Layout: pgstore.SharedTable, Prefix: "recs_"})
	e := engine(t, s)
	ctx := context.Background()
	if _, err := e.Define(ctx, schema.Definition{Name: "odd", Indexes: []string{"/we?ird'key"}, Schema: &schema.JSONSchema{Type: "object"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{"we?ird'key":"v%d","a/b":%d}`, i, i)
		if _, err := e.Insert(ctx, "odd", json.RawMessage(body)); err != nil {
			t.Fatal(err)
		}
	}
	f := query.Gte("/we?ird'key", "v2")
	srt := []query.SortField{{Field: "/a~1b", Desc: true}}
	p, err := e.List(ctx, "odd", query.Query{Filter: &f, Sort: srt, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := e.List(ctx, "odd", query.Query{Filter: &f, Sort: srt, Limit: 2, Cursor: p.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 2 || len(p2.Items) != 1 || p2.NextCursor != "" {
		t.Fatalf("pages: %d then %d (cursor %q)", len(p.Items), len(p2.Items), p2.NextCursor)
	}
	// Tampered cursors carrying SQL must be inert: cursor values are always
	// rendered as escaped literals (numbers are regex-checked).
	evil := query.Cursor{Sort: "/a~1b:d,$id:a", Keys: []query.CursorKey{
		{Value: json.RawMessage(`"1); DROP TABLE recs_records; --"`)},
		{Value: json.RawMessage(`"x' OR '1'='1"`)},
	}}
	if _, err := e.List(ctx, "odd", query.Query{Sort: srt, Cursor: evil.Encode()}); err != nil {
		t.Fatalf("tampered cursor should be inert, got %v", err)
	}
	evil.Keys[0].Value = json.RawMessage(`3`)
	p3, err := e.List(ctx, "odd", query.Query{Sort: srt, Cursor: evil.Encode()})
	if err != nil {
		t.Fatal(err)
	}
	if len(p3.Items) != 3 { // a/b in {2,1,0} sorts after 3 descending
		t.Fatalf("want 3 items after a/b=3, got %d", len(p3.Items))
	}
	all, err := e.List(ctx, "odd", query.Query{})
	if err != nil || len(all.Items) != 5 {
		t.Fatalf("table must survive: %d items, %v", len(all.Items), err)
	}
}

// A tenant with the wrong number of values for the tenant columns names no
// tenant: it fails closed, like a missing one.
func TestTenantArity(t *testing.T) {
	s := open(t, isolated(t, dsn(t)), pgstore.Options{Layout: pgstore.SharedTable, TenantColumns: suiteColumns, Tenant: storetest.TenantOf})
	for _, tenant := range [][]string{{"acme"}, {"acme", "alice", "x"}} {
		if _, err := s.ListDefinitions(storetest.WithTenant(tenant...)); !errors.Is(err, store.ErrNoTenant) {
			t.Fatalf("%q: want ErrNoTenant, got %v", tenant, err)
		}
	}
	if _, err := s.ListDefinitions(storetest.WithTenant("acme", "alice")); err != nil {
		t.Fatal(err)
	}
}
