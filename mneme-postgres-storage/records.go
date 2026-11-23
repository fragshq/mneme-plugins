package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/store"
)

type recordRow struct {
	ID            string          `db:"id"`
	SchemaVersion int             `db:"schema_version"`
	Data          json.RawMessage `db:"data"`
	CreatedAt     time.Time       `db:"created_at"`
	UpdatedAt     time.Time       `db:"updated_at"`
	Labels        []string        `db:"labels"`
}

const recordColumns = `id, schema_version, data, created_at, updated_at, labels`

func (r recordRow) toRecord(recordType string) store.Record {
	rec := store.Record{
		RecordType:    recordType,
		ID:            r.ID,
		SchemaVersion: r.SchemaVersion,
		Data:          r.Data,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
		Labels:        r.Labels,
	}
	return rec.Normalized()
}

// builder returns a SQL builder configured for the store.
func (s *Store) builder() *sqlBuilder { return &sqlBuilder{gin: s.opts.IndexAllFields} }

// matching returns the WHERE clause selecting the tenant's records of one
// type that match filter (nil: all), or store.ErrNoTenant. Every operation
// on records builds its WHERE with it, so they all scope alike.
func (s *Store) matching(ctx context.Context, b *sqlBuilder, recordType string, filter *query.Filter) (string, error) {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return "", err
	}
	conds := []string{s.tenantWhere(b, tenant)}
	if s.shared() {
		conds = append(conds, "record_type = "+b.arg(recordType, "::text"))
	}
	if filter != nil {
		c, err := b.filter(*filter, false)
		if err != nil {
			return "", err
		}
		conds = append(conds, c)
	}
	return strings.Join(conds, " AND "), nil
}

// collect runs a SELECT and scans every row with scan. A missing
// table-per-type table reads as no rows. It never returns a nil slice
// without an error.
func collect[T any](ctx context.Context, db DB, sql string, args []any, scan pgx.RowToFunc[T]) ([]T, error) {
	// pgx may report an error when sending or while reading rows.
	rows, err := db.Query(ctx, sql, args...)
	out := []T(nil)
	if err == nil {
		out, err = pgx.CollectRows(rows, scan)
	}
	if isUndefinedTable(err) {
		return []T{}, nil
	}
	return out, err // CollectRows returns a non-nil slice when it succeeds
}

// selectRecords runs a records SELECT (recordColumns) and decodes its rows.
func (s *Store) selectRecords(ctx context.Context, recordType string, sql string, args []any) ([]store.Record, error) {
	rows, err := collect(ctx, s.db, sql, args, pgx.RowToStructByName[recordRow])
	if err != nil {
		return nil, err
	}
	out := make([]store.Record, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.toRecord(recordType))
	}
	return out, nil
}

// InsertRecord implements store.RecordStore.
func (s *Store) InsertRecord(ctx context.Context, r store.Record) error {
	r = r.Normalized()
	tenant, err := s.tenant(ctx)
	if err != nil {
		return err
	}
	b := s.builder()
	cols := []string{s.tenantCols}
	vals := []string{tenantArgs(b, tenant)}
	if s.shared() {
		cols = append(cols, "record_type")
		vals = append(vals, b.arg(r.RecordType, ""))
	}
	cols = append(cols, "id", "schema_version", "data", "created_at", "updated_at", "labels")
	vals = append(vals, b.arg(r.ID, ""), b.arg(r.SchemaVersion, ""), b.arg(r.Data, "::jsonb"),
		b.arg(r.CreatedAt, ""), b.arg(r.UpdatedAt, ""), b.jsonArg(r.Labels))
	table := s.RecordTable(r.RecordType)
	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, ident(table), strings.Join(cols, ", "), strings.Join(vals, ", "))
	_, err = s.db.Exec(ctx, sql, b.args...)
	if isUndefinedTable(err) {
		// Written before any definition: create the table, then retry.
		if err = s.ensureRecordTable(ctx, table); err == nil {
			_, err = s.db.Exec(ctx, sql, b.args...)
		}
	}
	if isUniqueViolation(err) {
		return store.RecordExists(r.RecordType, r.ID)
	}
	return err
}

// GetRecord implements store.RecordStore.
func (s *Store) GetRecord(ctx context.Context, recordType string, id string) (store.Record, error) {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, nil)
	if err != nil {
		return store.Record{}, err
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s AND id = %s`,
		recordColumns, ident(s.RecordTable(recordType)), where, b.arg(id, "::text"))
	rs, err := s.selectRecords(ctx, recordType, sql, b.args)
	if err != nil {
		return store.Record{}, err
	}
	if len(rs) == 0 {
		return store.Record{}, store.RecordNotFound(recordType, id)
	}
	return rs[0], nil
}

// changed maps a write's outcome: no row (or no table) is ErrNotFound.
func changed(tag pgconn.CommandTag, err error, recordType string, id string) error {
	if err != nil && !isUndefinedTable(err) {
		return err
	}
	if err != nil || tag.RowsAffected() == 0 {
		return store.RecordNotFound(recordType, id)
	}
	return nil
}

// UpdateRecord implements store.RecordStore.
func (s *Store) UpdateRecord(ctx context.Context, r store.Record) error {
	r = r.Normalized()
	b := s.builder()
	where, err := s.matching(ctx, b, r.RecordType, nil)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`UPDATE %s SET schema_version = %s, data = %s, created_at = %s, updated_at = %s, labels = %s
		WHERE %s AND id = %s`, ident(s.RecordTable(r.RecordType)),
		b.arg(r.SchemaVersion, ""), b.arg(r.Data, "::jsonb"), b.arg(r.CreatedAt, ""), b.arg(r.UpdatedAt, ""), b.jsonArg(r.Labels),
		where, b.arg(r.ID, "::text"))
	tag, err := s.db.Exec(ctx, sql, b.args...)
	return changed(tag, err, r.RecordType, r.ID)
}

// DeleteRecord implements store.RecordStore.
func (s *Store) DeleteRecord(ctx context.Context, recordType string, id string) error {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, nil)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`DELETE FROM %s WHERE %s AND id = %s`, ident(s.RecordTable(recordType)), where, b.arg(id, "::text"))
	tag, err := s.db.Exec(ctx, sql, b.args...)
	return changed(tag, err, recordType, id)
}

// listSQL translates a normalized query into one SELECT (see sql.go). It
// fetches Limit+1 rows, so the caller knows whether more follow.
func (s *Store) listSQL(ctx context.Context, recordType string, q query.Query) (string, []any, error) {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, q.Filter)
	if err != nil {
		return "", nil, err
	}
	keys, err := sortKeys(q.Sort)
	if err != nil {
		return "", nil, err
	}
	if q.Cursor != "" {
		cur, err := query.DecodeCursor(q.Cursor, q.Sort)
		if err != nil {
			return "", nil, err
		}
		c, err := b.after(keys, cur)
		if err != nil {
			return "", nil, err
		}
		where += " AND " + c
	}
	order := []string{}
	for _, k := range keys {
		dir := "ASC"
		if k.desc {
			dir = "DESC"
		}
		order = append(order, k.expr+" "+dir)
	}
	sql := fmt.Sprintf(`SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d`,
		recordColumns, ident(s.RecordTable(recordType)), where, strings.Join(order, ", "), q.Limit+1)
	return sql, b.args, nil
}

// ListRecords implements store.RecordStore with the SELECT from listSQL.
func (s *Store) ListRecords(ctx context.Context, recordType string, q query.Query) (query.Page[store.Record], error) {
	sql, args, err := s.listSQL(ctx, recordType, q)
	if err != nil {
		return query.Page[store.Record]{}, err
	}
	rs, err := s.selectRecords(ctx, recordType, sql, args)
	if err != nil {
		return query.Page[store.Record]{}, err
	}
	page := query.Page[store.Record]{Items: rs}
	if len(rs) > q.Limit {
		page.Items = rs[:q.Limit]
		d, err := store.NewDoc(page.Items[q.Limit-1])
		if err != nil {
			return query.Page[store.Record]{}, err
		}
		page.NextCursor = query.CursorFor(q.Sort, d).Encode()
	}
	return page, nil
}

// ListLabels implements store.RecordStore with one aggregate query: the
// matching records' labels unnested, grouped and ordered in byte order
// (COLLATE "C"), as the reference sorts them.
func (s *Store) ListLabels(ctx context.Context, recordType string, filter *query.Filter) ([]store.LabelCount, error) {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, filter)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(`SELECT l.label, count(*)::int FROM %s, jsonb_array_elements_text(labels) AS l(label)
		WHERE %s GROUP BY l.label ORDER BY l.label COLLATE "C"`, ident(s.RecordTable(recordType)), where)
	return collect(ctx, s.db, sql, b.args, pgx.RowToStructByPos[store.LabelCount])
}

// CountRecords implements store.RecordStore with one count(*) query over the
// same conditions as ListRecords.
func (s *Store) CountRecords(ctx context.Context, recordType string, filter *query.Filter) (int, error) {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, filter)
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf(`SELECT count(*)::int FROM %s WHERE %s`, ident(s.RecordTable(recordType)), where)
	n, err := collect(ctx, s.db, sql, b.args, pgx.RowTo[int])
	if err != nil || len(n) == 0 { // no rows: the table is missing
		return 0, err
	}
	return n[0], nil
}
