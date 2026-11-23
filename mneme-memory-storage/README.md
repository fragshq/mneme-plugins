# mneme-memory-storage

An in-memory storage backend for [Mneme](https://github.com/fragshq/mneme). It implements `store.SchemaStore`, `store.RecordStore` and `store.SequenceProvider`, and passes the Mneme conformance suites (`storetest.Run` and `storetest.RunTenancy`). Nothing is persisted: the data lives as long as the `Store` value.

```sh
go get github.com/fragshq/mneme-plugins/mneme-memory-storage
```

```go
import memorystore "github.com/fragshq/mneme-plugins/mneme-memory-storage"
```

```go
s := memorystore.New(memorystore.Options{
	// optional multi-tenancy: each tenant gets its own isolated store,
	// and an operation whose ctx names no tenant fails with store.ErrNoTenant
	Tenant: func(ctx context.Context) string { return tenantFrom(ctx) },
})
eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
```

## How it works

Each tenant gets its own instance of the core's `store/memstore`, the reference implementation, so filtering, sorting and cursors follow Mneme's reference semantics exactly. Without `Options.Tenant`, everything lives in one instance.

- **Tenants are created on their first write.** Reads in an unknown tenant see an empty store and allocate nothing.
- **`Tenants(ctx)`** lists the tenants holding data (each as its values, sorted), and **`DropTenant(values...)`** discards one tenant's data and frees its memory. A tenant may have any number of values (say, organization and user); each combination is its own space.
- **Copies in and out.** Definitions and records are copied on write and on read, so callers can't mutate stored state.

## Compared with the core's `memstore`

`store/memstore` is the reference behaviour that the core's own tests use, and it has no tenancy. This plugin adds tenancy, so it can be a drop-in backend wherever the database plugins go, such as the workbench's `MNEME_STORAGE=memory`.

## Trade-offs

- **List cost is O(n) per call.** `ListRecords` and `ListLabels` evaluate the query in Go over every record of the type. There are no indexes, and a definition's `Indexes` are validated by the core but create nothing.
- **Memory only grows** until records are deleted or a tenant is dropped.
- **One lock per tenant.** Writes within a tenant are serialized. Tenants don't contend, except briefly when a tenant is first created.

## Tests

```sh
go test -race ./...
```

They need no external services. The module depends on the core, [`github.com/fragshq/mneme`](https://github.com/fragshq/mneme). To develop against a local checkout of the core, add `replace github.com/fragshq/mneme => ../../mneme` to `go.mod` (don't commit it).
