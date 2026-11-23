package pgstore

import (
	"context"

	"github.com/fragshq/mneme/query"
)

// ListSQL exposes the SQL ListRecords would run, for plan assertions.
func (s *Store) ListSQL(ctx context.Context, recordType string, q query.Query) (string, []any, error) {
	return s.listSQL(ctx, recordType, q)
}
