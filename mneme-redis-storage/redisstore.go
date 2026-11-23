// Package redisstore is a Mneme storage backend on Redis. It implements
// store.SchemaStore, store.RecordStore and store.SequenceProvider.
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	s := redisstore.New(rdb, redisstore.Options{Prefix: "myapp"})
//	eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
//
// # Key layout
//
// All keys of one namespace share a Redis Cluster hash tag, so the
// multi-key Lua script used by PutDefinition is cluster-safe:
//
//	{ns}:def:<name>     hash  version -> Definition JSON
//	{ns}:latest         hash  name -> latest version
//	{ns}:rec:<type>     hash  id -> {"schema_version":..,"data":..,"created_at":..,"updated_at":..,"labels":[..]}
//	{ns}:seq:<type>     hash  <name>\x00<scope> -> counter
//
// ns is Options.Prefix, followed by ":" and Options.Tenant(ctx) when set.
// Tenant is the hook for multi-tenancy: derive the tenant from the
// context and every tenant gets its own, isolated key space (and slot). With
// Tenant set, a context that names no tenant is rejected
// (store.ErrNoTenant) rather than falling back to the bare prefix.
//
// # Queries
//
// Redis cannot filter or sort on arbitrary JSON fields natively, so
// ListRecords streams the record type's hash with HSCAN and evaluates the
// query in Go with Mneme's reference semantics (store.Paginate); ListLabels
// does the same (store.CountLabels). Every list call therefore costs O(n)
// in the number of records of that type: fine for small and medium
// collections, not for millions of records per type.
//
// # Tenants
//
// Tenants lists the namespaces holding record types, for administration
// tools. It scans the key space, so it is meant for occasional use.
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
)

// Options configures a Store.
type Options struct {
	// Prefix starts every key. Default "mneme".
	Prefix string
	// Tenant, when set, enables tenancy: it is called on every operation and
	// its result is appended to Prefix as a namespace, isolating tenants
	// carried in the context. An empty result is rejected with
	// store.ErrNoTenant. It must return the same value for every call within
	// one logical scope.
	Tenant store.TenantFunc
	// ScanCount is the HSCAN batch size used by ListRecords. Default 500.
	ScanCount int64
}

// Store is a Redis-backed Mneme store. It is safe for concurrent use.
type Store struct {
	rdb  redis.UniversalClient
	opts Options
}

var _ store.Store = (*Store)(nil)

// New returns a Store using rdb, which may be a single-node, sentinel or
// cluster client.
func New(rdb redis.UniversalClient, opts Options) *Store {
	if opts.Prefix == "" {
		opts.Prefix = "mneme"
	}
	if opts.ScanCount <= 0 {
		opts.ScanCount = 500
	}
	return &Store{rdb: rdb, opts: opts}
}

// ns returns the hash-tagged namespace for ctx, e.g. "{mneme:tenant-a}",
// or store.ErrNoTenant.
func (s *Store) ns(ctx context.Context) (string, error) {
	t, err := store.Tenant(ctx, s.opts.Tenant)
	if err != nil {
		return "", err
	}
	return s.nsFor(t), nil
}

// nsFor returns a tenant's hash-tagged namespace: "{prefix}" without
// tenancy, "{prefix:<store.TenantKey>}" otherwise. TenantKey escapes the
// braces, so the hash tag always ends where the namespace does.
func (s *Store) nsFor(tenant []string) string {
	if len(tenant) == 0 {
		return "{" + s.opts.Prefix + "}"
	}
	return "{" + s.opts.Prefix + ":" + store.TenantKey(tenant) + "}"
}

func defKey(ns string, name string) string       { return ns + ":def:" + name }
func latestKey(ns string) string                 { return ns + ":latest" }
func recKey(ns string, recordType string) string { return ns + ":rec:" + recordType }
func seqKey(ns string, recordType string) string { return ns + ":seq:" + recordType }

// ---- SchemaStore ----------------------------------------------------------

// putDefinition stores a version only if it is new, and advances the
// "latest" pointer, atomically.
//
//	KEYS[1] = def hash, KEYS[2] = latest hash
//	ARGV[1] = version, ARGV[2] = definition JSON, ARGV[3] = name
var putDefinition = redis.NewScript(`
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 0 then
  return 0
end
local cur = tonumber(redis.call('HGET', KEYS[2], ARGV[3]) or '0')
if tonumber(ARGV[1]) > cur then
  redis.call('HSET', KEYS[2], ARGV[3], ARGV[1])
end
return 1
`)

// PutDefinition implements store.SchemaStore.
func (s *Store) PutDefinition(ctx context.Context, d schema.Definition) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	keys := []string{defKey(ns, d.Name), latestKey(ns)}
	ok, err := putDefinition.Run(ctx, s.rdb, keys, d.Version, string(raw), d.Name).Int()
	if err != nil {
		return err
	}
	if ok == 0 {
		return store.VersionExists(d.Name, d.Version)
	}
	return nil
}

func decodeDefinition(raw string) (schema.Definition, error) {
	d := schema.Definition{}
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return d, fmt.Errorf("redisstore: corrupt definition: %w", err)
	}
	return d, nil
}

// GetDefinition implements store.SchemaStore.
func (s *Store) GetDefinition(ctx context.Context, name string, version int) (schema.Definition, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return schema.Definition{}, err
	}
	field := strconv.Itoa(version)
	if version == 0 {
		v, err := s.rdb.HGet(ctx, latestKey(ns), name).Result()
		if errors.Is(err, redis.Nil) {
			return schema.Definition{}, store.TypeNotFound(name)
		}
		if err != nil {
			return schema.Definition{}, err
		}
		field = v
	}
	raw, err := s.rdb.HGet(ctx, defKey(ns, name), field).Result()
	if errors.Is(err, redis.Nil) {
		return schema.Definition{}, store.VersionNotFound(name, version)
	}
	if err != nil {
		return schema.Definition{}, err
	}
	return decodeDefinition(raw)
}

// ListDefinitions implements store.SchemaStore.
func (s *Store) ListDefinitions(ctx context.Context) ([]schema.Definition, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := s.rdb.HGetAll(ctx, latestKey(ns)).Result()
	if err != nil {
		return nil, err
	}
	names := slices.Sorted(maps.Keys(latest))
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(names))
	for i, n := range names {
		cmds[i] = pipe.HGet(ctx, defKey(ns, n), latest[n])
	}
	if _, err := pipe.Exec(ctx); err != nil { // an empty pipeline is a no-op
		return nil, err
	}
	out := make([]schema.Definition, 0, len(names))
	for _, c := range cmds {
		d, err := decodeDefinition(c.Val())
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// ListVersions implements store.SchemaStore.
func (s *Store) ListVersions(ctx context.Context, name string) ([]schema.Definition, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return nil, err
	}
	all, err := s.rdb.HGetAll(ctx, defKey(ns, name)).Result()
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, store.TypeNotFound(name)
	}
	out := make([]schema.Definition, 0, len(all))
	for _, raw := range all {
		d, err := decodeDefinition(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	slices.SortFunc(out, func(a schema.Definition, b schema.Definition) int { return a.Version - b.Version })
	return out, nil
}

// ---- RecordStore ----------------------------------------------------------

// recordValue is the stored form of a record; the type and ID live in the key.
type recordValue struct {
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Labels        []string        `json:"labels"`
}

func encodeRecord(r store.Record) (string, error) {
	r = r.Normalized()
	raw, err := json.Marshal(recordValue{SchemaVersion: r.SchemaVersion, Data: r.Data, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Labels: r.Labels})
	return string(raw), err
}

func decodeRecord(recordType string, id string, raw string) (store.Record, error) {
	v := recordValue{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return store.Record{}, fmt.Errorf("redisstore: corrupt record %s/%s: %w", recordType, id, err)
	}
	r := store.Record{
		RecordType:    recordType,
		ID:            id,
		SchemaVersion: v.SchemaVersion,
		Data:          v.Data,
		CreatedAt:     v.CreatedAt,
		UpdatedAt:     v.UpdatedAt,
		Labels:        v.Labels,
	}
	return r.Normalized(), nil
}

// InsertRecord implements store.RecordStore. HSETNX makes the uniqueness
// check and the write a single atomic step.
func (s *Store) InsertRecord(ctx context.Context, r store.Record) error {
	raw, err := encodeRecord(r)
	if err != nil {
		return err
	}
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	ok, err := s.rdb.HSetNX(ctx, recKey(ns, r.RecordType), r.ID, raw).Result()
	if err != nil {
		return err
	}
	if !ok {
		return store.RecordExists(r.RecordType, r.ID)
	}
	return nil
}

// GetRecord implements store.RecordStore.
func (s *Store) GetRecord(ctx context.Context, recordType string, id string) (store.Record, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return store.Record{}, err
	}
	raw, err := s.rdb.HGet(ctx, recKey(ns, recordType), id).Result()
	if errors.Is(err, redis.Nil) {
		return store.Record{}, store.RecordNotFound(recordType, id)
	}
	if err != nil {
		return store.Record{}, err
	}
	return decodeRecord(recordType, id, raw)
}

// replaceIfExists overwrites a hash field only when it already exists.
//
//	KEYS[1] = record hash, ARGV[1] = id, ARGV[2] = record JSON
var replaceIfExists = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// UpdateRecord implements store.RecordStore. A Lua script makes the
// existence check and the overwrite atomic, so a missing record is never
// created.
func (s *Store) UpdateRecord(ctx context.Context, r store.Record) error {
	raw, err := encodeRecord(r)
	if err != nil {
		return err
	}
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	ok, err := replaceIfExists.Run(ctx, s.rdb, []string{recKey(ns, r.RecordType)}, r.ID, raw).Int()
	if err != nil {
		return err
	}
	if ok == 0 {
		return store.RecordNotFound(r.RecordType, r.ID)
	}
	return nil
}

// DeleteRecord implements store.RecordStore.
func (s *Store) DeleteRecord(ctx context.Context, recordType string, id string) error {
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	n, err := s.rdb.HDel(ctx, recKey(ns, recordType), id).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return store.RecordNotFound(recordType, id)
	}
	return nil
}

// ListRecords implements store.RecordStore. See the package documentation
// for its cost.
func (s *Store) ListRecords(ctx context.Context, recordType string, q query.Query) (query.Page[store.Record], error) {
	docs, err := s.scanRecords(ctx, recordType)
	if err != nil {
		return query.Page[store.Record]{}, err
	}
	return store.Paginate(docs, q)
}

// ListLabels implements store.RecordStore. Like ListRecords, it reads the
// whole record type and evaluates the filter in Go.
func (s *Store) ListLabels(ctx context.Context, recordType string, filter *query.Filter) ([]store.LabelCount, error) {
	docs, err := s.scanRecords(ctx, recordType)
	if err != nil {
		return nil, err
	}
	return store.CountLabels(docs, filter), nil
}

// CountRecords implements store.RecordStore. Like ListRecords, it reads the
// whole record type and evaluates the filter in Go.
func (s *Store) CountRecords(ctx context.Context, recordType string, filter *query.Filter) (int, error) {
	docs, err := s.scanRecords(ctx, recordType)
	if err != nil {
		return 0, err
	}
	return store.Count(docs, filter), nil
}

// scanRecords reads every record of a type with HSCAN, decoded as Docs for
// the store's query helpers.
func (s *Store) scanRecords(ctx context.Context, recordType string) ([]*store.Doc, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return nil, err
	}
	key := recKey(ns, recordType)
	docs := []*store.Doc{}
	seen := map[string]bool{} // HSCAN may return a field more than once
	// The iterator pages through HSCAN, yielding each field then its value.
	it := s.rdb.HScan(ctx, key, 0, "", s.opts.ScanCount).Iterator()
	for it.Next(ctx) {
		id := it.Val()
		if !it.Next(ctx) {
			break
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		r, err := decodeRecord(recordType, id, it.Val())
		if err != nil {
			return nil, err
		}
		d, err := store.NewDoc(r)
		if err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, it.Err()
}

// DeleteRecords implements store.RecordStore: the type's records are one
// hash, removed with a single DEL.
func (s *Store) DeleteRecords(ctx context.Context, recordType string) error {
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	return s.rdb.Del(ctx, recKey(ns, recordType)).Err()
}

// deleteDefinitions removes every version and the latest pointer of a
// record type atomically.
//
//	KEYS[1] = def hash, KEYS[2] = latest hash, ARGV[1] = name
var deleteDefinitions = redis.NewScript(`
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)

// DeleteDefinitions implements store.SchemaStore.
func (s *Store) DeleteDefinitions(ctx context.Context, name string) error {
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	ok, err := deleteDefinitions.Run(ctx, s.rdb, []string{defKey(ns, name), latestKey(ns)}, name).Int()
	if err != nil {
		return err
	}
	if ok == 0 {
		return store.TypeNotFound(name)
	}
	return nil
}

// ---- SequenceProvider -----------------------------------------------------

// Next implements store.SequenceProvider with HINCRBY, which is atomic.
func (s *Store) Next(ctx context.Context, recordType string, name string, scope string) (int64, error) {
	ns, err := s.ns(ctx)
	if err != nil {
		return 0, err
	}
	return s.rdb.HIncrBy(ctx, seqKey(ns, recordType), name+"\x00"+scope, 1).Result()
}

// DeleteSequences implements store.SequenceProvider: the type's counters
// are one hash, removed with a single DEL.
func (s *Store) DeleteSequences(ctx context.Context, recordType string) error {
	ns, err := s.ns(ctx)
	if err != nil {
		return err
	}
	return s.rdb.Del(ctx, seqKey(ns, recordType)).Err()
}
