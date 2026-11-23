package redisstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fragshq/mneme"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
	"github.com/fragshq/mneme/store/storetest"

	redisstore "github.com/fragshq/mneme-plugins/mneme-redis-storage"
)

var seq atomic.Int64

// clients returns the Redis servers to test against: always an in-process
// miniredis, plus a real server when MNEME_REDIS_ADDR is set, e.g.
//
//	docker run -d --rm -p 56379:6379 redis:7
//	MNEME_REDIS_ADDR=localhost:56379 go test ./...
func clients(t *testing.T) map[string]redis.UniversalClient {
	out := map[string]redis.UniversalClient{}
	mr := miniredis.RunT(t)
	out["miniredis"] = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if addr := os.Getenv("MNEME_REDIS_ADDR"); addr != "" {
		out["redis"] = redis.NewClient(&redis.Options{Addr: addr})
	}
	for _, c := range out {
		t.Cleanup(func() { c.Close() })
	}
	return out
}

// freshPrefix gives each subtest an empty key space and removes it afterwards.
func freshPrefix(t *testing.T, rdb redis.UniversalClient) string {
	p := fmt.Sprintf("mneme-test-%d-%d", os.Getpid(), seq.Add(1))
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, "{"+p+"*", 200).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
	})
	return p
}

func TestConformance(t *testing.T) {
	for name, rdb := range clients(t) {
		t.Run(name, func(t *testing.T) {
			storetest.Run(t, func(t *testing.T) store.Store {
				return redisstore.New(rdb, redisstore.Options{Prefix: freshPrefix(t, rdb), ScanCount: 3})
			})
		})
	}
}

// TestTenancy runs the tenancy conformance suite with Options.Tenant.
func TestTenancy(t *testing.T) {
	for name, rdb := range clients(t) {
		t.Run(name, func(t *testing.T) {
			storetest.RunTenancy(t, func(t *testing.T, tenant store.TenantFunc, _ []string) store.Store {
				return redisstore.New(rdb, redisstore.Options{Prefix: freshPrefix(t, rdb), Tenant: tenant})
			})
		})
	}
}

// With Tenant set, an operation whose context names no tenant must not
// create, change or remove any key. (TestTenancy checks the error and that
// the data survives; this checks Redis itself.)
func TestRejectedCallsTouchNoKeys(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := redisstore.New(rdb, redisstore.Options{Prefix: "t", Tenant: storetest.TenantOf})
	calls := storetest.CallsWithoutTenant(t, s, "acme", "alice")
	keys := mr.Keys()
	for name, call := range calls {
		if err := call(); !errors.Is(err, store.ErrNoTenant) {
			t.Errorf("%s without a tenant: want store.ErrNoTenant, got %v", name, err)
		}
	}
	if after := mr.Keys(); strings.Join(after, ",") != strings.Join(keys, ",") {
		t.Fatalf("rejected calls must not touch Redis: keys %v became %v", keys, after)
	}
}

// TestStoredValueFormat pins the stored JSON keys, which match the column
// names used by the SQL plugin.
func TestStoredValueFormat(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := redisstore.New(rdb, redisstore.Options{Prefix: "fmt"})
	e, err := mneme.New(mneme.Config{Schemas: s, Records: s, Sequences: s})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := e.Define(ctx, schema.Definition{Name: "note", Schema: &schema.JSONSchema{Type: "object"}}); err != nil {
		t.Fatal(err)
	}
	r, err := e.Insert(ctx, "note", json.RawMessage(`{"x":1}`), "b", "a")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rdb.HGet(ctx, "{fmt}:rec:note", r.ID).Result()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || string(got["data"]) != `{"x":1}` || string(got["schema_version"]) != "1" ||
		got["created_at"] == nil || got["updated_at"] == nil || string(got["labels"]) != `["a","b"]` {
		t.Fatalf("unexpected stored value: %s", raw)
	}
}
