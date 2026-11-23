# mneme-postgres-storage

A Postgres storage backend for [Mneme](https://github.com/fragshq/mneme), built on [pgx](https://github.com/jackc/pgx) v5. It implements `store.SchemaStore`, `store.RecordStore` and `store.SequenceProvider`, and passes the Mneme conformance suite (`storetest.Run`) in every layout and option combination.

```sh
go get github.com/fragshq/mneme-plugins/mneme-postgres-storage
```

```go
import pgstore "github.com/fragshq/mneme-plugins/mneme-postgres-storage"
```

```go
pool, _ := pgxpool.New(ctx, dsn)
s, err := pgstore.New(ctx, pool, pgstore.Options{
	Layout: pgstore.SharedTable, // or pgstore.TablePerType
	// optional multi-tenancy: every operation is scoped to the tenant in ctx,
	// one value per tenant column, and an operation whose ctx names no
	// (complete) tenant fails with store.ErrNoTenant
	TenantColumns: []string{"organization_id", "user_id"}, // default: one "tenant" column
	Tenant:        func(ctx context.Context) []string { return []string{orgFrom(ctx), userFrom(ctx)} },
})
eng, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
```

`New` takes a `pgstore.DB`: a `*pgxpool.Pool`, or a `pgx.Tx` to run the store inside the application's own transaction (not concurrently). Gorm users can keep Gorm for their own tables and open a `pgxpool.Pool` on the same database for Mneme.

## Layouts

The application chooses the layout; Mneme never sees it.

| | `TablePerType` | `SharedTable` |
|---|---|---|
| Records live in | `<Prefix>rec_<type>`, one table per record type | `<Prefix>records`, with a `record_type` discriminator column |
| Table created | when the type is first defined, in the same transaction as its definition | once, by `New` |
| Definition `Indexes` | expression indexes on the type's table | validated, but **no index created** |
| Suits | single tenant, a modest number of types | multi-tenant, where tenants define many types |

`Store.RecordTable(type)` returns the table name, for applications that query it directly (reporting, BI). Names longer than 63 bytes are truncated and get a hash suffix.

## Columns

Technical fields are real columns, and the user payload is one `jsonb` column:

```
tenant          text COLLATE "C"   -- '' without Options.Tenant
record_type     text COLLATE "C"   -- SharedTable only
id              text COLLATE "C"
schema_version  integer
created_at      timestamptz
updated_at      timestamptz
labels          jsonb              -- array of strings
data            jsonb              -- the record, always an object
PRIMARY KEY (tenant, [record_type,] id)
```

`<Prefix>definitions` (tenant columns, `name, version` primary key; `id_strategy` is `json`, not `jsonb`, so segment configs keep their exact bytes) and `<Prefix>sequences` (tenant columns, `record_type, name, scope`) are created by `New`. `Prefix` defaults to `mneme_`.

## Tenancy

Tenancy is decided at bootstrap, by whether `Options.Tenant` is set:

| `Options.Tenant` | Behavior |
|---|---|
| not set | Single tenant. Every tenant column is `''`, and contexts are never inspected. |
| set | Multi-tenant. `Tenant(ctx)` scopes everything: each tenant has its own definitions (two tenants may define `risk` v1 differently), records and sequence counters. If it returns no tenant, an empty value, or not exactly one value per tenant column, the operation fails with **`store.ErrNoTenant`** (also `mneme.ErrNoTenant`), and nothing is read or written. There is no shared default to fall back to. |

A tenant can span several columns: set `Options.TenantColumns`, such as `["organization_id", "user_id"]`, and return one value per column, in order. Each column is `text COLLATE "C"`, the columns lead every primary key and index, and each full combination of values is one isolated space: `(acme, alice)` and `(acme, bob)` share nothing. The default is a single column, `tenant`. The columns are fixed when the tables are created, like the layout.

Typically, middleware takes the tenant from a verified credential (such as a JWT claim) and puts it in the request context. Work outside a request, such as background jobs, must build a context that names its tenant explicitly. The tenant columns lead every primary key and index. The schema is the same either way, so turning tenancy on later needs no migration. Rows written before that stay under `''` until reassigned.

Deleting a record type (`DeleteRecords`, `DeleteSequences`, `DeleteDefinitions`) deletes the tenant's rows. With `TablePerType` the table and its indexes stay, since other tenants may share it, and a later definition with the same name reuses them.

`Tenants(ctx)` lists the tenants that hold at least one record type, each as its values, in byte order; rows of a store without tenancy show as an empty tenant. It reads across tenants whatever `Options.Tenant` is, so it's meant for administration tools.

## Indexes

Every record table gets:

- **Primary key** `(tenant, [record_type,] id)`, which serves `$id` filters and sorts.
- **`(tenant, [record_type,] created_at, id)`** and **`(… updated_at, id)`**. The default sort (`$createdAt`, `$id`) and `$updatedAt` sorts page through these, with no sort step.
- **GIN on `labels`**, which serves `$labels` `contains`.
- **GIN on `data`** (`jsonb_path_ops`), only with `Options.IndexAllFields`. It serves `eq`, `in` and non-string `contains` on any field, at the cost of a large index and slower writes.

With `TablePerType`, each field listed in a definition's `Indexes` (pointer or dotted form) gets two expression indexes:

- **`(tenant, data #> path)`** (jsonb), for `eq`, `in` and numeric ranges.
- **`(tenant, (data #>> path) COLLATE "C")`** (byte-order text), for string ranges and `prefix`.

Indexes are created with `IF NOT EXISTS` and never dropped. A later version that stops listing a field leaves its index in place. They're built inside the definition's transaction, so on a large table, defining a version with a new index blocks writes to that table while the index builds. With `SharedTable`, that means every type.

With `SharedTable`, `Indexes` are validated (a bad field still fails the definition) but create nothing. Every type of every tenant would add its own indexes to the one table, slowing every write to it and bloating it without bound. Filters on JSON fields still work there, by scanning the tenant's rows of that type. `IndexAllFields` remains the one opt-in index on `data`.

`TestIndexesServeQueries` asserts with `EXPLAIN` that the indexes are actually picked, and that `SharedTable` has none on `data` fields.

## Query translation (`sql.go`)

Filters, sorting and keyset pagination run natively in SQL, following Mneme's reference semantics. `TestFiltersMatchReference` checks every op, alone and under `NOT`, against memstore, for both `ListRecords` and `ListLabels`.

`ListLabels` is one aggregate query over the same `WHERE` as a list: it unnests `labels` with `jsonb_array_elements_text`, groups by label and orders with `COLLATE "C"` (byte order, like the reference). It reads every matching row, so its cost grows with the records the filter selects.

- **NULL handling:** a missing field makes a leaf NULL. Under `NOT`, leaves are wrapped in `COALESCE(…, false)`, so `NOT` of a missing field is true. Elsewhere only `AND`/`OR` sit above them and `WHERE` treats NULL as false, so leaves stay bare and the planner can match them to indexes.
- **Index-friendly leaves.** Each one is exact, not an approximation:
  - **Numbers:** a number above `c` is written `j > c AND j < 'false'::jsonb`, because jsonb orders null < strings < numbers < booleans.
  - **Strings:** comparisons use `(data #>> path) COLLATE "C"` (byte order).
  - **`prefix`:** gets a `>= prefix` lower bound.
  - **Scalar `contains` on an array:** written as `@>`.
  - **GIN pre-filters (`IndexAllFields`):** a `data @> {…}` containment, ANDed before the exact test. It's emitted only outside `NOT`, and only for pointers that can't step into an array.
- **Sorting:** a JSON sort field expands into four NULL-free `ORDER BY` components: type rank, string value (byte order), numeric value and raw jsonb. Meta fields are single columns.
- **Keyset pagination:** when every component sorts in the same direction, it's a row comparison `(k1, k2, …) > (c1, c2, …)`, which a matching btree serves as a range scan. Mixed directions expand to an `OR` chain.
- **Parameters:** every value, including client-supplied cursor values, is a numbered parameter (`$n`), so nothing a client sends is ever spliced into SQL. Numeric cursor values are shape-checked so malformed ones return `ErrInvalid`. The only inlined strings are JSON pointer tokens, as escaped literals (`literal()`), because index expressions must read the same in DDL and queries for the planner to match them. `TestHostileFieldNamesAndCursors` covers this.

## Writes

- **Definitions:** one transaction, under an advisory lock on the record table. It inserts the definition (a duplicate returns `ErrConflict`), creates the `TablePerType` table and its declared indexes. If any step fails, nothing is left behind.
- **Records written before their type is defined:** the conformance suite does this; the engine never does. With `TablePerType`, the first insert creates the table. Reads, updates and deletes on a type with no table behave as if it were empty.
- **Sequences:** an atomic `INSERT … ON CONFLICT DO UPDATE … RETURNING`.
- **Updates and deletes:** plain statements. When no row is affected, they return `ErrNotFound`.

## Run

```sh
docker run -d --rm --name mneme-pg-test -p 55432:5432 -e POSTGRES_PASSWORD=pw postgres:17
export MNEME_PG_DSN='postgres://postgres:pw@localhost:55432/postgres?sslmode=disable'
go test -v ./...      # tests SKIP without MNEME_PG_DSN; -v shows whether they ran
docker stop mneme-pg-test
```

The module depends on the core, [`github.com/fragshq/mneme`](https://github.com/fragshq/mneme). To develop against a local checkout of the core, add `replace github.com/fragshq/mneme => ../../mneme` to `go.mod` (don't commit it).
