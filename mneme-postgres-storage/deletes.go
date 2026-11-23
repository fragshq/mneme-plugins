package pgstore

import (
	"context"
	"fmt"

	"github.com/fragshq/mneme/store"
)

// DeleteRecords implements store.RecordStore. It deletes the tenant's rows
// of the type. With TablePerType the table and its indexes stay, since
// other tenants may share it; a later definition of the same name reuses
// them. A missing table holds nothing to delete.
func (s *Store) DeleteRecords(ctx context.Context, recordType string) error {
	b := s.builder()
	where, err := s.matching(ctx, b, recordType, nil)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, ident(s.RecordTable(recordType)), where), b.args...)
	if isUndefinedTable(err) {
		return nil
	}
	return err
}

// DeleteDefinitions implements store.SchemaStore.
func (s *Store) DeleteDefinitions(ctx context.Context, name string) error {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return err
	}
	b := s.builder()
	tag, err := s.db.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s AND name = %s`,
		ident(s.definitionsTable()), s.tenantWhere(b, tenant), b.arg(name, "::text")), b.args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.TypeNotFound(name)
	}
	return nil
}

// DeleteSequences implements store.SequenceProvider.
func (s *Store) DeleteSequences(ctx context.Context, recordType string) error {
	tenant, err := s.tenant(ctx)
	if err != nil {
		return err
	}
	b := s.builder()
	_, err = s.db.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s AND record_type = %s`,
		ident(s.sequencesTable()), s.tenantWhere(b, tenant), b.arg(recordType, "::text")), b.args...)
	return err
}
