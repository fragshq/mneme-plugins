package pgstore

import (
	"context"
	"fmt"
)

// Next implements store.SequenceProvider with an atomic upsert.
func (s *Store) Next(ctx context.Context, recordType string, name string, scope string) (int64, error) {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return 0, err
	}
	b := s.builder()
	sql := fmt.Sprintf(`
		INSERT INTO %s AS seq (%s, record_type, name, scope, value) VALUES (%s, %s, %s, %s, 1)
		ON CONFLICT (%s, record_type, name, scope) DO UPDATE SET value = seq.value + 1
		RETURNING value`, ident(s.sequencesTable()), s.tenantCols,
		tenantArgs(b, tenant), b.arg(recordType, ""), b.arg(name, ""), b.arg(scope, ""), s.tenantCols)
	n := int64(0)
	err = s.db.QueryRow(ctx, sql, b.args...).Scan(&n)
	return n, err
}
