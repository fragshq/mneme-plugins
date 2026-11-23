// Package pgstore is a Mneme storage backend on Postgres (via pgx). It
// implements store.SchemaStore, store.RecordStore and store.SequenceProvider.
//
//	pool, _ := pgxpool.New(ctx, dsn)
//	s, err := pgstore.New(ctx, pool, pgstore.Options{Layout: pgstore.SharedTable})
//	eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
//
// # Layouts
//
// TablePerType gives every record type its own table, created (with its
// indexes) in the same transaction that stores the type's first definition.
// SharedTable keeps every record type in one table, told apart by a
// record_type discriminator column.
//
// In both layouts the technical fields are real columns (the tenant
// columns, id, schema_version, created_at, updated_at, labels) and the user
// payload is a single jsonb column, data.
//
// # Tenancy
//
// Every table carries the tenant columns (Options.TenantColumns, by default
// one column, tenant), which lead its primary key. A tenant may span several
// columns, such as organization_id and user_id: each full combination of
// values is one isolated space. Setting Options.Tenant enables tenancy: its
// values (one per column, in order) scope every operation, so definitions,
// sequences and records of one tenant are invisible to the others, and an
// operation whose context names no tenant, or a partial one, fails with
// store.ErrNoTenant instead of falling back to a shared default. Without it,
// every tenant column holds the empty string. Tenants lists the tenants holding record
// types, for administration tools.
//
// # Indexes
//
// Each record table has primary-key and (created_at, id) / (updated_at, id)
// btree indexes, so the default sort and $updatedAt sorts page through an
// index, and a GIN index on labels for $labels contains.
//
// With TablePerType, each field listed in a definition's Indexes gets a
// jsonb expression index (eq, in, numeric ranges) and a text expression
// index in byte order (string ranges, prefix). With SharedTable, Indexes
// are validated but create nothing: per-type indexes on the one shared
// table would multiply with every type and tenant.
//
// Options.IndexAllFields adds one GIN index on data, used by eq, in and
// non-string contains filters on any field, in either layout.
package pgstore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/fragshq/mneme/store"
)

// DB is the connection a Store runs on. *pgxpool.Pool is the usual choice.
// A pgx.Tx works too (Begin then opens savepoints), which lets an
// application run a Store inside its own transaction; like *pgx.Conn, it
// must then not be used concurrently.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Layout decides where a record type's records live.
type Layout int

const (
	// TablePerType gives every record type a dedicated table,
	// "<Prefix>rec_<type>". It suits single-tenant deployments with a modest
	// number of record types.
	TablePerType Layout = iota + 1
	// SharedTable keeps every record type in "<Prefix>records", with a
	// record_type discriminator column. It suits multi-tenant deployments,
	// where tenants define many types and a table per type would explode.
	SharedTable
)

// Options configures a Store.
type Options struct {
	// Layout is required.
	Layout Layout
	// Prefix starts every table name. It must be a plain SQL identifier
	// (letters, digits, '_'). Default "mneme_".
	Prefix string
	// TenantColumns names the columns a tenant spans, in order: one per value
	// Tenant returns, e.g. ["organization_id", "user_id"]. They are text
	// COLLATE "C" columns leading every primary key. Each must be a plain SQL
	// identifier and must not clash with a technical column. Default
	// ["tenant"]. Like Layout, it is decided when the tables are created.
	TenantColumns []string
	// Tenant, when set, enables tenancy: it is called on every operation and
	// scopes it to the returned tenant, one value per TenantColumns entry. A
	// missing, partial or wrongly sized result is rejected with
	// store.ErrNoTenant. It must return the same values for every call within
	// one logical scope. When nil, every tenant column holds the empty string.
	Tenant store.TenantFunc
	// IndexAllFields adds a GIN (jsonb_path_ops) index on data, so eq, in
	// and non-string contains filters can use an index on any field, at the
	// cost of a larger index and slower writes.
	IndexAllFields bool
}

// Store is a Postgres-backed Mneme store. It is safe for concurrent use
// when its DB is.
type Store struct {
	db   DB
	opts Options
	// tenantCols is the quoted, comma-separated list of tenant columns.
	tenantCols string
}

var _ store.Store = (*Store)(nil)

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// technicalColumns are the columns a tenant column must not be named after.
var technicalColumns = map[string]bool{
	"id": true, "record_type": true, "schema_version": true, "created_at": true, "updated_at": true,
	"labels": true, "data": true, "name": true, "version": true, "title": true, "description": true,
	"id_strategy": true, "indexes": true, "schema": true, "scope": true, "value": true,
}

// New creates the definition and sequence tables (and, with SharedTable,
// the records table) if they do not exist, and returns a Store.
func New(ctx context.Context, db DB, opts Options) (*Store, error) {
	if opts.Layout != TablePerType && opts.Layout != SharedTable {
		return nil, errors.New("pgstore: Options.Layout must be TablePerType or SharedTable")
	}
	if opts.Prefix == "" {
		opts.Prefix = "mneme_"
	}
	if !identRe.MatchString(opts.Prefix) {
		return nil, fmt.Errorf("pgstore: Options.Prefix %q must match %s", opts.Prefix, identRe)
	}
	if len(opts.TenantColumns) == 0 {
		opts.TenantColumns = []string{"tenant"}
	}
	quoted := []string{}
	seen := map[string]bool{}
	for _, c := range opts.TenantColumns {
		if !identRe.MatchString(c) || technicalColumns[strings.ToLower(c)] || seen[strings.ToLower(c)] {
			return nil, fmt.Errorf("pgstore: Options.TenantColumns: %q must be a distinct identifier (%s) that is not a technical column", c, identRe)
		}
		seen[strings.ToLower(c)] = true
		quoted = append(quoted, ident(c))
	}
	s := &Store{db: db, opts: opts, tenantCols: strings.Join(quoted, ", ")}
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if err := lock(ctx, tx, s.definitionsTable()); err != nil {
			return err
		}
		ddl := []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
				%s
				name text COLLATE "C" NOT NULL,
				version integer NOT NULL,
				title text NOT NULL DEFAULT '',
				description text NOT NULL DEFAULT '',
				id_strategy json,
				indexes jsonb,
				schema jsonb NOT NULL,
				created_at timestamptz NOT NULL,
				PRIMARY KEY (%s, name, version))`, ident(s.definitionsTable()), s.tenantColumnsDDL(), s.tenantCols),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
				%s
				record_type text COLLATE "C" NOT NULL,
				name text COLLATE "C" NOT NULL,
				scope text COLLATE "C" NOT NULL,
				value bigint NOT NULL,
				PRIMARY KEY (%s, record_type, name, scope))`, ident(s.sequencesTable()), s.tenantColumnsDDL(), s.tenantCols),
		}
		if err := execAll(ctx, tx, ddl); err != nil {
			return err
		}
		if opts.Layout == SharedTable {
			return s.createRecordTable(ctx, tx, s.sharedTable())
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pgstore: create tables: %w", err)
	}
	return s, nil
}

// RecordTable returns the (unquoted) table holding recordType's records, for
// applications that query it directly, e.g. for reporting.
func (s *Store) RecordTable(recordType string) string {
	if s.shared() {
		return s.sharedTable()
	}
	return pgName(s.opts.Prefix + "rec_" + recordType)
}

func (s *Store) shared() bool             { return s.opts.Layout == SharedTable }
func (s *Store) sharedTable() string      { return s.opts.Prefix + "records" }
func (s *Store) definitionsTable() string { return s.opts.Prefix + "definitions" }
func (s *Store) sequencesTable() string   { return s.opts.Prefix + "sequences" }

// tenant returns the tenant scoping ctx, one value per tenant column: all empty
// when tenancy is disabled, and store.ErrNoTenant when it is enabled but ctx
// names no complete tenant of the right size.
func (s *Store) tenant(ctx context.Context) ([]string, error) {
	t, err := store.Tenant(ctx, s.opts.Tenant)
	switch {
	case err != nil:
		return nil, err
	case s.opts.Tenant == nil:
		return make([]string, len(s.opts.TenantColumns)), nil
	case len(t) != len(s.opts.TenantColumns):
		return nil, fmt.Errorf("%w: got %d tenant values for columns %s", store.ErrNoTenant, len(t), s.tenantCols)
	}
	return t, nil
}

// tenantColumnsDDL declares the tenant columns, each followed by a comma.
func (s *Store) tenantColumnsDDL() string {
	b := strings.Builder{}
	for _, c := range s.opts.TenantColumns {
		b.WriteString(ident(c) + ` text COLLATE "C" NOT NULL DEFAULT '',` + "\n")
	}
	return b.String()
}

// tenantWhere returns the conditions selecting tenant's rows, its values
// added as parameters.
func (s *Store) tenantWhere(b *sqlBuilder, tenant []string) string {
	conds := make([]string, len(tenant))
	for i, v := range tenant {
		conds[i] = ident(s.opts.TenantColumns[i]) + " = " + b.arg(v, "::text")
	}
	return strings.Join(conds, " AND ")
}

// tenantArgs adds tenant's values as parameters and returns their
// placeholders, comma-separated, in tenant column order.
func tenantArgs(b *sqlBuilder, tenant []string) string {
	ph := make([]string, len(tenant))
	for i, v := range tenant {
		ph[i] = b.arg(v, "")
	}
	return strings.Join(ph, ", ")
}

// pgName fits a name into Postgres' 63-byte identifier limit, replacing the
// tail of long names with a hash so distinct names stay distinct.
func pgName(name string) string {
	if len(name) <= 63 {
		return name
	}
	sum := sha1.Sum([]byte(name))
	return name[:50] + "_" + hex.EncodeToString(sum[:])[:12]
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func execAll(ctx context.Context, tx pgx.Tx, stmts []string) error {
	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// lock serializes DDL on one object across processes for the rest of tx.
func lock(ctx context.Context, tx pgx.Tx, key string) error {
	h := fnv.New64a()
	h.Write([]byte("mneme/pgstore:" + key))
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(h.Sum64()))
	return err
}

func pgCode(err error) string {
	pg := &pgconn.PgError{}
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

func isUniqueViolation(err error) bool { return pgCode(err) == "23505" }

// isUndefinedTable reports a missing table-per-type table: the record type
// was never defined (or written), so it has no records.
func isUndefinedTable(err error) bool { return pgCode(err) == "42P01" }
