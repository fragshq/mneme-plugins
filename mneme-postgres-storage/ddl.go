package pgstore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/fragshq/mneme/errs"
	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/schema"
)

// createRecordTable creates a record table and its base indexes. tx must
// hold the table's lock.
func (s *Store) createRecordTable(ctx context.Context, tx pgx.Tx, table string) error {
	discriminator, scope := "", s.tenantCols
	if s.shared() {
		discriminator, scope = `record_type text COLLATE "C" NOT NULL,`, s.tenantCols+", record_type"
	}
	ddl := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			%s
			%s
			id text COLLATE "C" NOT NULL,
			schema_version integer NOT NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			labels jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(labels) = 'array'),
			data jsonb NOT NULL CHECK (jsonb_typeof(data) = 'object'),
			PRIMARY KEY (%s, id))`, ident(table), s.tenantColumnsDDL(), discriminator, scope),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s, created_at, id)`,
			ident(pgName(table+"_created")), ident(table), scope),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s, updated_at, id)`,
			ident(pgName(table+"_updated")), ident(table), scope),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING gin (labels jsonb_path_ops)`,
			ident(pgName(table+"_labels")), ident(table)),
	}
	if s.opts.IndexAllFields {
		ddl = append(ddl, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING gin (data jsonb_path_ops)`,
			ident(pgName(table+"_data")), ident(table)))
	}
	return execAll(ctx, tx, ddl)
}

// createFieldIndexes creates the indexes a definition asks for: per field, a
// jsonb expression index and a byte-order text expression index, each led by
// the tenant columns. DDL takes no parameters, so the field path is inlined as escaped
// literals. tx must hold the table's lock.
//
// With SharedTable the fields are validated but no index is created: every
// type of every tenant would add its own indexes to the one table, slowing
// all writes to it and bloating it without bound.
func (s *Store) createFieldIndexes(ctx context.Context, tx pgx.Tx, table string, d schema.Definition) error {
	for _, f := range d.Indexes {
		field, err := query.NormalizeField(f)
		if err != nil {
			return errs.Invalidf("record type %s: index %q: %v", d.Name, f, err)
		}
		if query.IsMeta(field) || s.shared() {
			continue // meta fields are columns with their own indexes
		}
		j, t, err := fieldExprs(field)
		if err != nil {
			return err
		}
		sum := sha1.Sum([]byte(d.Name + "\x00" + field))
		base := table + "_f" + hex.EncodeToString(sum[:])[:12]
		err = execAll(ctx, tx, []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s)`, ident(pgName(base+"_j")), ident(table), s.tenantCols, j),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s COLLATE "C")`, ident(pgName(base+"_t")), ident(table), s.tenantCols, t),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ensureRecordTable creates a table-per-type table outside of PutDefinition,
// for records written to a type whose table does not exist yet.
func (s *Store) ensureRecordTable(ctx context.Context, table string) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if err := lock(ctx, tx, table); err != nil {
			return err
		}
		return s.createRecordTable(ctx, tx, table)
	})
}
