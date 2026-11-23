package redisstore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store/storetest"

	redisstore "github.com/fragshq/mneme-plugins/mneme-redis-storage"
)

func TestTenants(t *testing.T) {
	for name, rdb := range clients(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			prefix := freshPrefix(t, rdb)
			named := redisstore.New(rdb, redisstore.Options{Prefix: prefix, Tenant: storetest.TenantOf})
			bare := redisstore.New(rdb, redisstore.Options{Prefix: prefix})
			// A lookalike prefix must not leak into the listing.
			other := redisstore.New(rdb, redisstore.Options{Prefix: prefix + "x", Tenant: storetest.TenantOf})
			t.Cleanup(func() {
				iter := rdb.Scan(ctx, 0, "{"+prefix+"x*", 200).Iterator()
				for iter.Next(ctx) {
					rdb.Del(ctx, iter.Val())
				}
			})

			if got, err := bare.Tenants(ctx); err != nil || len(got) != 0 {
				t.Fatalf("empty store: %v %v", got, err)
			}
			def := schema.Definition{Name: "note", Version: 1, Schema: &schema.JSONSchema{Type: "object"}}
			for _, put := range []struct {
				s      *redisstore.Store
				tenant []string
			}{{named, []string{"globex"}}, {named, []string{"acme"}}, {named, []string{"acme", "a/lice"}}, {bare, nil}, {other, []string{"initech"}}} {
				if err := put.s.PutDefinition(storetest.WithTenant(put.tenant...), def); err != nil {
					t.Fatal(err)
				}
			}
			for _, s := range []*redisstore.Store{named, bare} {
				got, err := s.Tenants(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if want := `[[] ["acme"] ["acme" "a/lice"] ["globex"]]`; fmt.Sprintf("%q", got) != want {
					t.Fatalf("want the bare space then the tenants sorted, %s, got %q", want, got)
				}
			}
		})
	}
}
