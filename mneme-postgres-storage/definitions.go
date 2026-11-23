package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fragshq/mneme/idgen"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
)

// definitionRow is one stored version. pgx's JSON codec encodes and decodes
// the JSON columns, and SQL NULL reads back as nil.
type definitionRow struct {
	Name        string             `db:"name"`
	Version     int                `db:"version"`
	Title       string             `db:"title"`
	Description string             `db:"description"`
	IDStrategy  *idgen.Spec        `db:"id_strategy"`
	Indexes     []string           `db:"indexes"`
	Schema      *schema.JSONSchema `db:"schema"`
	CreatedAt   time.Time          `db:"created_at"`
}

const definitionColumns = `name, version, title, description, id_strategy, indexes, schema, created_at`

// queryDefinitions runs a definitions SELECT and decodes its rows.
func (s *Store) queryDefinitions(ctx context.Context, sql string, args ...any) ([]schema.Definition, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	found, err := pgx.CollectRows(rows, pgx.RowToStructByName[definitionRow])
	if err != nil {
		return nil, err
	}
	out := make([]schema.Definition, 0, len(found))
	for _, r := range found {
		out = append(out, schema.Definition{Name: r.Name, Version: r.Version, Title: r.Title, Description: r.Description,
			IDStrategy: r.IDStrategy, Indexes: r.Indexes, Schema: r.Schema, CreatedAt: r.CreatedAt.UTC()})
	}
	return out, nil
}

// PutDefinition implements store.SchemaStore. In one transaction it stores
// the definition and creates the record table (TablePerType) and the
// indexes the definition asks for.
func (s *Store) PutDefinition(ctx context.Context, d schema.Definition) error {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return err
	}
	table := s.RecordTable(d.Name)
	err = pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if err := lock(ctx, tx, table); err != nil {
			return err
		}
		b := s.builder()
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (%s, name, version, title, description, id_strategy, indexes, schema, created_at)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`, ident(s.definitionsTable()), s.tenantCols, tenantArgs(b, tenant),
			b.arg(d.Name, ""), b.arg(d.Version, ""), b.arg(d.Title, ""), b.arg(d.Description, ""),
			b.arg(d.IDStrategy, ""), b.arg(d.Indexes, ""), b.arg(d.Schema, ""), b.arg(d.CreatedAt, "")), b.args...)
		if err != nil {
			return err
		}
		if !s.shared() {
			if err := s.createRecordTable(ctx, tx, table); err != nil {
				return err
			}
		}
		return s.createFieldIndexes(ctx, tx, table, d)
	})
	if isUniqueViolation(err) {
		return store.VersionExists(d.Name, d.Version)
	}
	return err
}

// GetDefinition implements store.SchemaStore.
func (s *Store) GetDefinition(ctx context.Context, name string, version int) (schema.Definition, error) {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return schema.Definition{}, err
	}
	b := s.builder()
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s AND name = %s`,
		definitionColumns, ident(s.definitionsTable()), s.tenantWhere(b, tenant), b.arg(name, "::text"))
	if version != 0 {
		sql += " AND version = " + b.arg(version, "::integer")
	}
	ds, err := s.queryDefinitions(ctx, sql+" ORDER BY version DESC LIMIT 1", b.args...)
	if err != nil {
		return schema.Definition{}, err
	}
	if len(ds) == 0 {
		return schema.Definition{}, store.VersionNotFound(name, version)
	}
	return ds[0], nil
}

// ListDefinitions implements store.SchemaStore.
func (s *Store) ListDefinitions(ctx context.Context) ([]schema.Definition, error) {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return nil, err
	}
	b := s.builder()
	return s.queryDefinitions(ctx, fmt.Sprintf(`SELECT DISTINCT ON (name) %s FROM %s WHERE %s ORDER BY name, version DESC`,
		definitionColumns, ident(s.definitionsTable()), s.tenantWhere(b, tenant)), b.args...)
}

// ListVersions implements store.SchemaStore.
func (s *Store) ListVersions(ctx context.Context, name string) ([]schema.Definition, error) {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return nil, err
	}
	b := s.builder()
	ds, err := s.queryDefinitions(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE %s AND name = %s ORDER BY version`,
		definitionColumns, ident(s.definitionsTable()), s.tenantWhere(b, tenant), b.arg(name, "::text")), b.args...)
	if err == nil && len(ds) == 0 {
		err = store.TypeNotFound(name)
	}
	return ds, err
}
