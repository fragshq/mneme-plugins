package pgstore

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Tenants returns the tenants that hold at least one record type, each as
// its values (one per tenant column), in byte order, whatever Options.Tenant
// is. The rows of a Store without tenancy (every tenant column empty) are
// reported as an empty tenant. It is meant for administration tools: it
// reads across tenants.
func (s *Store) Tenants(ctx context.Context) ([][]string, error) {
	order := []string{}
	for _, c := range s.opts.TenantColumns {
		order = append(order, ident(c)+` COLLATE "C"`)
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s GROUP BY %s ORDER BY %s`,
		s.tenantCols, ident(s.definitionsTable()), s.tenantCols, strings.Join(order, ", ")))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]string, error) {
		values := make([]string, len(s.opts.TenantColumns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := row.Scan(dest...); err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(values, func(v string) bool { return v != "" }) {
			return []string{}, nil // no tenancy
		}
		return values, nil
	})
}
