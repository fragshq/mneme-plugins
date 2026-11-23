package redisstore

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/fragshq/mneme/store"
)

// Tenants returns the tenants that hold at least one record type, each as
// its values, sorted by store.TenantKey, whatever Options.Tenant is. An
// empty tenant stands for the bare key space ({prefix}), the one a Store
// without Tenant uses. It SCANs the key space (on every master of a
// cluster), so it suits administration tools, not hot paths.
func (s *Store) Tenants(ctx context.Context) ([][]string, error) {
	named := "{" + s.opts.Prefix + ":" // a tenant namespace, up to the tenant
	pattern := globEscape(named) + "*}:latest"
	mu := sync.Mutex{}
	found := map[string]bool{}
	scan := func(ctx context.Context, c redis.Cmdable) error {
		iter := c.Scan(ctx, 0, pattern, s.opts.ScanCount).Iterator()
		for iter.Next(ctx) {
			rest := strings.TrimPrefix(iter.Val(), named) // the pattern guarantees the prefix
			if t, ok := strings.CutSuffix(rest, "}:latest"); ok && t != "" {
				mu.Lock()
				found[t] = true
				mu.Unlock()
			}
		}
		return iter.Err()
	}
	if cc, ok := s.rdb.(*redis.ClusterClient); ok {
		if err := cc.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return scan(ctx, c) }); err != nil {
			return nil, err
		}
	} else if err := scan(ctx, s.rdb); err != nil {
		return nil, err
	}
	n, err := s.rdb.Exists(ctx, latestKey(s.nsFor(nil))).Result()
	if err != nil {
		return nil, err
	}
	if n > 0 {
		found[""] = true // TenantKey of no values
	}
	out := make([][]string, 0, len(found))
	for _, key := range slices.Sorted(maps.Keys(found)) {
		t, err := store.ParseTenantKey(key)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// globEscape escapes the characters Redis' glob matching treats specially.
func globEscape(s string) string {
	b := strings.Builder{}
	for _, r := range s {
		if strings.ContainsRune(`*?[]\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
