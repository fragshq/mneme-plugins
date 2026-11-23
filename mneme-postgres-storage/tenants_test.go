package pgstore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store/storetest"

	pgstore "github.com/fragshq/mneme-plugins/mneme-postgres-storage"
)

func TestTenants(t *testing.T) {
	base := dsn(t)
	for _, layout := range []pgstore.Layout{pgstore.TablePerType, pgstore.SharedTable} {
		db := isolated(t, base)
		ctx := context.Background()
		cols := []string{"org_id", "user_id"}
		named := open(t, db, pgstore.Options{Layout: layout, TenantColumns: cols, Tenant: storetest.TenantOf})
		bare := open(t, db, pgstore.Options{Layout: layout, TenantColumns: cols})
		if got, err := bare.Tenants(ctx); err != nil || len(got) != 0 {
			t.Fatalf("empty store: %v %v", got, err)
		}
		def := schema.Definition{Name: "note", Version: 1, Schema: &schema.JSONSchema{Type: "object"}}
		for _, put := range []struct {
			s      *pgstore.Store
			tenant []string
		}{{named, []string{"globex", "x"}}, {named, []string{"acme", "b"}}, {named, []string{"acme", "a"}}, {named, []string{"Zeta", "a"}}, {bare, nil}} {
			if err := put.s.PutDefinition(storetest.WithTenant(put.tenant...), def); err != nil {
				t.Fatal(err)
			}
		}
		for _, s := range []*pgstore.Store{named, bare} {
			got, err := s.Tenants(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if want := `[[] ["Zeta" "a"] ["acme" "a"] ["acme" "b"] ["globex" "x"]]`; fmt.Sprintf("%q", got) != want {
				t.Fatalf("want every tenant once, in byte order, %s, got %q", want, got)
			}
		}
	}
}
