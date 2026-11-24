# mneme-plugins

Storage backends for [Mneme](https://github.com/fragshq/mneme). Each plugin implements `store.SchemaStore`, `store.RecordStore` and `store.SequenceProvider`, so any of them can be handed to the engine as is:

```go
eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
```

## Plugins

| Plugin | Backend | Persistence | Queries | Multi-tenancy |
|---|---|---|---|---|
| [`mneme-memory-storage`](mneme-memory-storage) | Go memory | none, lives as long as the `Store` | evaluated in Go, O(n) per list | one isolated store per tenant |
| [`mneme-postgres-storage`](mneme-postgres-storage) | Postgres ([pgx](https://github.com/jackc/pgx) v5) | durable | native SQL, with indexes | tenant columns, one or several |
| [`mneme-redis-storage`](mneme-redis-storage) | Redis (single, sentinel, cluster) | durable | evaluated in Go, O(n) per list | one key space per tenant |

Every plugin passes the Mneme conformance suite (`storetest.Run`), and every plugin supports optional multi-tenancy: set a `Tenant` function in its options and each tenant gets its own definitions, records and sequence counters. An operation whose context names no tenant fails with `store.ErrNoTenant`.

### Which one?

- **Memory:** tests, demos, local development, and anywhere you'd use the core's `store/memstore` but need tenancy.
- **Postgres:** production. Filtering, sorting and pagination run in SQL, and definitions can declare indexes.
- **Redis:** small and medium collections where you already run Redis. Listing reads a whole record type, so it isn't meant for millions of records per type.

Each plugin's README covers its layout, trade-offs and tests in detail.

## Install

Each plugin is its own Go module. Get only the one you need:

```sh
go get github.com/fragshq/mneme-plugins/mneme-memory-storage
go get github.com/fragshq/mneme-plugins/mneme-postgres-storage
go get github.com/fragshq/mneme-plugins/mneme-redis-storage
```

## Development

Run the tests from a plugin's directory:

```sh
cd mneme-memory-storage && go test -race ./...
```

The memory and Redis plugins need no external services (Redis tests use an in-process [miniredis](https://github.com/alicebob/miniredis)). The Postgres tests skip unless `MNEME_PG_DSN` is set. See each plugin's README for how to run them against a real server.

To develop against a local checkout of the core, add `replace github.com/fragshq/mneme => ../../mneme` to the plugin's `go.mod` (don't commit it).

## License

[GNU Affero General Public License v3.0](LICENSE).
