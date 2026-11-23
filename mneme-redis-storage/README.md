# mneme-redis-storage

A Redis storage backend for [Mneme](https://github.com/fragshq/mneme). It implements `store.SchemaStore`, `store.RecordStore` and `store.SequenceProvider`, and passes the Mneme conformance suite (`storetest.Run`).

```sh
go get github.com/fragshq/mneme-plugins/mneme-redis-storage
```

```go
import redisstore "github.com/fragshq/mneme-plugins/mneme-redis-storage"
```

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // any redis.UniversalClient: single, sentinel, cluster
s := redisstore.New(rdb, redisstore.Options{
	Prefix: "myapp",
	// optional multi-tenancy: each tenant gets its own isolated key space,
	// and an operation whose ctx names no tenant fails with store.ErrNoTenant
	// a tenant may have several values, e.g. organization and user
	Tenant: func(ctx context.Context) []string { return []string{orgFrom(ctx), userFrom(ctx)} },
})
eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
```

## Key layout

Every key in a namespace shares a Redis Cluster hash tag `{ns}`, so the multi-key Lua script stays cluster-safe. Here `ns` is the `Prefix`, plus `:<key>` when a tenant function is set, where the key is `store.TenantKey(Tenant(ctx))`: the values escaped and joined with `/`, so distinct tenants never share a namespace.

Setting `Tenant` turns on tenancy at bootstrap. From then on, an operation whose context yields an empty tenant fails with **`store.ErrNoTenant`** (also `mneme.ErrNoTenant`) and touches no key, instead of falling back to the bare prefix.

| Key | Type | Content |
|---|---|---|
| `{ns}:def:<type>` | hash | version → definition JSON |
| `{ns}:latest` | hash | type → latest version |
| `{ns}:rec:<type>` | hash | id → `{"schema_version":…,"data":…,"created_at":…,"updated_at":…,"labels":[…]}` |
| `{ns}:seq:<type>` | hash | `<name>\x00<scope>` → counter |

These operations are atomic:

- **New definition versions:** one Lua script does `HSETNX` on the version and advances the latest pointer.
- **Record inserts:** `HSETNX`, so a duplicate ID is detected and rejected in the same step.
- **Record updates:** a Lua script checks the record exists and overwrites it, so a missing record is never created.
- **Record deletes:** `HDEL`.
- **Sequences:** `HINCRBY`.
- **Deleting a record type:** its records and its counters are one hash each, removed with `DEL`; a Lua script removes its definitions and its latest pointer together.

## Trade-offs

- **List cost is O(n) per call.** Redis can't filter or sort on arbitrary JSON fields, so `ListRecords` reads the record type's hash with `HSCAN` and evaluates the query in Go with Mneme's reference semantics (`store.Paginate`), and `ListLabels` does the same (`store.CountLabels`). That's fine for small and medium collections, but not for millions of records per type. Native filtering would need RediSearch/RedisJSON.
- **One cluster slot per namespace.** Each namespace (for example, each tenant) lives in a single slot. Spread load across tenants with `Tenant`.

## Listing tenants

`Tenants(ctx)` returns the tenants that hold at least one record type, each as its values, sorted, with an empty tenant for the bare key space (`{prefix}`) a store without `Tenant` uses. It works whatever `Tenant` is set to, so an administration tool can list tenants through any store on the same prefix. It `SCAN`s the key space (on every master of a cluster), so it's for occasional use, not hot paths.

## Tests

The tests always run against an in-process [miniredis](https://github.com/alicebob/miniredis). To also run them against a real server:

```sh
docker run -d --rm --name mneme-redis-test -p 56379:6379 redis:7
MNEME_REDIS_ADDR=localhost:56379 go test -race ./...
docker stop mneme-redis-test
```

The module depends on the core, [`github.com/fragshq/mneme`](https://github.com/fragshq/mneme). To develop against a local checkout of the core, add `replace github.com/fragshq/mneme => ../../mneme` to `go.mod` (don't commit it).
