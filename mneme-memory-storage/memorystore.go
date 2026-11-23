// Package memorystore is an in-memory Mneme storage backend with optional
// multi-tenancy. Nothing is persisted: data lives as long as the Store value.
//
// Each tenant gets its own core memstore, the reference implementation, so
// queries follow Mneme's reference semantics exactly. Lists evaluate the
// query in Go over every record of the type, so each list costs O(n).
package memorystore

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
	"github.com/fragshq/mneme/store/memstore"
)

// Options configures a Store.
type Options struct {
	// Tenant, when set, turns on tenancy: each tenant it returns (one value
	// per tenancy field, any number of fields) gets its own isolated
	// definitions, records and sequences, and an operation whose context
	// yields no tenant, or a partial one, fails with store.ErrNoTenant. When
	// nil, the store is single-tenant.
	Tenant store.TenantFunc
}

// Store implements store.Store. It is safe for concurrent use. The zero
// value is not usable; call New.
type Store struct {
	opts    Options
	mu      sync.RWMutex
	tenants map[string]*memstore.Store
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New(opts Options) *Store {
	return &Store{opts: opts, tenants: map[string]*memstore.Store{}}
}

// Tenants returns the tenants holding data, each as its values, sorted by
// store.TenantKey. A single-tenant store reports one empty tenant once it
// holds data.
func (s *Store) Tenants(context.Context) ([][]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([][]string, 0, len(s.tenants))
	for _, key := range slices.Sorted(maps.Keys(s.tenants)) {
		t, err := store.ParseTenantKey(key)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// DropTenant discards everything the tenant made of values holds and frees
// its memory. It is a no-op for an unknown tenant.
func (s *Store) DropTenant(values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tenants, store.TenantKey(values))
}

// Whether an operation may create its tenant (see Store.tenant).
const (
	read  = false
	write = true
)

// tenant returns the context's tenant store. A write creates the tenant on
// its first use; a read of an unknown tenant gets an empty store that isn't
// kept, so reads never allocate state. Updates and deletes only change
// existing data, so they count as reads: on an unknown tenant they fail with
// store.ErrNotFound like on any missing data.
func (s *Store) tenant(ctx context.Context, create bool) (*memstore.Store, error) {
	values, err := store.Tenant(ctx, s.opts.Tenant)
	if err != nil {
		return nil, err
	}
	t := store.TenantKey(values)
	if !create {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if ts, ok := s.tenants[t]; ok {
			return ts, nil
		}
		return memstore.New(), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.tenants[t]
	if !ok {
		ts = memstore.New()
		s.tenants[t] = ts
	}
	return ts, nil
}

// on runs fn on the context's tenant store (see Store.tenant).
func on[T any](ctx context.Context, s *Store, create bool, fn func(m *memstore.Store) (T, error)) (T, error) {
	m, err := s.tenant(ctx, create)
	if err != nil {
		return *new(T), err
	}
	return fn(m)
}

// do is on for operations that return only an error.
func do(ctx context.Context, s *Store, create bool, fn func(m *memstore.Store) error) error {
	m, err := s.tenant(ctx, create)
	if err != nil {
		return err
	}
	return fn(m)
}

// PutDefinition implements store.SchemaStore.
func (s *Store) PutDefinition(ctx context.Context, d schema.Definition) error {
	return do(ctx, s, write, func(m *memstore.Store) error { return m.PutDefinition(ctx, d) })
}

// GetDefinition implements store.SchemaStore.
func (s *Store) GetDefinition(ctx context.Context, name string, version int) (schema.Definition, error) {
	return on(ctx, s, read, func(m *memstore.Store) (schema.Definition, error) { return m.GetDefinition(ctx, name, version) })
}

// ListDefinitions implements store.SchemaStore.
func (s *Store) ListDefinitions(ctx context.Context) ([]schema.Definition, error) {
	return on(ctx, s, read, func(m *memstore.Store) ([]schema.Definition, error) { return m.ListDefinitions(ctx) })
}

// ListVersions implements store.SchemaStore.
func (s *Store) ListVersions(ctx context.Context, name string) ([]schema.Definition, error) {
	return on(ctx, s, read, func(m *memstore.Store) ([]schema.Definition, error) { return m.ListVersions(ctx, name) })
}

// DeleteDefinitions implements store.SchemaStore.
func (s *Store) DeleteDefinitions(ctx context.Context, name string) error {
	return do(ctx, s, read, func(m *memstore.Store) error { return m.DeleteDefinitions(ctx, name) })
}

// InsertRecord implements store.RecordStore.
func (s *Store) InsertRecord(ctx context.Context, r store.Record) error {
	return do(ctx, s, write, func(m *memstore.Store) error { return m.InsertRecord(ctx, r) })
}

// GetRecord implements store.RecordStore.
func (s *Store) GetRecord(ctx context.Context, recordType string, id string) (store.Record, error) {
	return on(ctx, s, read, func(m *memstore.Store) (store.Record, error) { return m.GetRecord(ctx, recordType, id) })
}

// UpdateRecord implements store.RecordStore.
func (s *Store) UpdateRecord(ctx context.Context, r store.Record) error {
	return do(ctx, s, read, func(m *memstore.Store) error { return m.UpdateRecord(ctx, r) })
}

// DeleteRecord implements store.RecordStore.
func (s *Store) DeleteRecord(ctx context.Context, recordType string, id string) error {
	return do(ctx, s, read, func(m *memstore.Store) error { return m.DeleteRecord(ctx, recordType, id) })
}

// DeleteRecords implements store.RecordStore.
func (s *Store) DeleteRecords(ctx context.Context, recordType string) error {
	return do(ctx, s, read, func(m *memstore.Store) error { return m.DeleteRecords(ctx, recordType) })
}

// ListRecords implements store.RecordStore.
func (s *Store) ListRecords(ctx context.Context, recordType string, q query.Query) (query.Page[store.Record], error) {
	return on(ctx, s, read, func(m *memstore.Store) (query.Page[store.Record], error) { return m.ListRecords(ctx, recordType, q) })
}

// ListLabels implements store.RecordStore.
func (s *Store) ListLabels(ctx context.Context, recordType string, filter *query.Filter) ([]store.LabelCount, error) {
	return on(ctx, s, read, func(m *memstore.Store) ([]store.LabelCount, error) { return m.ListLabels(ctx, recordType, filter) })
}

// CountRecords implements store.RecordStore.
func (s *Store) CountRecords(ctx context.Context, recordType string, filter *query.Filter) (int, error) {
	return on(ctx, s, read, func(m *memstore.Store) (int, error) { return m.CountRecords(ctx, recordType, filter) })
}

// Next implements store.SequenceProvider.
func (s *Store) Next(ctx context.Context, recordType string, name string, scope string) (int64, error) {
	return on(ctx, s, write, func(m *memstore.Store) (int64, error) { return m.Next(ctx, recordType, name, scope) })
}

// DeleteSequences implements store.SequenceProvider.
func (s *Store) DeleteSequences(ctx context.Context, recordType string) error {
	return do(ctx, s, read, func(m *memstore.Store) error { return m.DeleteSequences(ctx, recordType) })
}
