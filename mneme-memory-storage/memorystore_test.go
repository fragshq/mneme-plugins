package memorystore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/fragshq/mneme/query"
	"github.com/fragshq/mneme/schema"
	"github.com/fragshq/mneme/store"
	"github.com/fragshq/mneme/store/storetest"

	memorystore "github.com/fragshq/mneme-plugins/mneme-memory-storage"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return memorystore.New(memorystore.Options{}) })
}

// TestTenancy also runs the conformance suite with every call pinned to one
// tenant.
func TestTenancy(t *testing.T) {
	storetest.RunTenancy(t, func(t *testing.T, tenant store.TenantFunc, _ []string) store.Store {
		return memorystore.New(memorystore.Options{Tenant: tenant})
	})
}

// TestTenantLifecycle checks that only writes create a tenant, and that
// DropTenant discards its data and nothing else.
func TestTenantLifecycle(t *testing.T) {
	ctx := context.Background()
	s := memorystore.New(memorystore.Options{Tenant: storetest.TenantOf})
	eng := storetest.Engine(t, s)
	as := storetest.WithTenant

	if _, err := eng.List(as("ghost"), "note", query.Query{}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("list in an unknown tenant: want ErrNotFound, got %v", err)
	}
	if _, err := eng.ListRecordTypes(as("ghost")); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRecord(as("ghost"), "note", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("delete in an unknown tenant: want ErrNotFound, got %v", err)
	}
	if got, _ := s.Tenants(ctx); len(got) != 0 {
		t.Fatalf("reads must not create tenants: %v", got)
	}

	note := schema.Definition{Name: "note", Schema: &schema.JSONSchema{Type: "object"}}
	for _, tenant := range []string{"globex", "acme"} {
		if _, err := eng.Define(as(tenant), note); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Insert(as(tenant), "note", json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s.Tenants(ctx); fmt.Sprint(got) != "[[acme] [globex]]" {
		t.Fatalf("tenants, sorted: got %v", got)
	}

	s.DropTenant("acme")
	if _, err := eng.List(as("acme"), "note", query.Query{}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("dropped tenant must be empty: got %v", err)
	}
	if p, err := eng.List(as("globex"), "note", query.Query{}); err != nil || len(p.Items) != 1 {
		t.Fatalf("dropping one tenant must leave the others: %+v, %v", p, err)
	}
	s.DropTenant("nobody")
}
