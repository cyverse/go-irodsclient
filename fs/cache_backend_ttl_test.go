package fs

import (
	"testing"
	"time"
)

func testCacheNamespaceDefaultTTL(t *testing.T, namespace CacheNamespace, waitForWrites func()) {
	t.Helper()

	if err := namespace.Set("default-ttl", "value", 0); err != nil {
		t.Fatalf("failed to set default-TTL value: %v", err)
	}
	if err := namespace.Set("expiring", "value", 10*time.Millisecond); err != nil {
		t.Fatalf("failed to set expiring value: %v", err)
	}
	waitForWrites()
	for _, key := range []string{"default-ttl", "expiring"} {
		if _, found, err := namespace.Get(key); err != nil {
			t.Fatalf("failed to get newly stored %q: %v", key, err)
		} else if !found {
			t.Fatalf("newly stored key %q was not found", key)
		}
	}

	time.Sleep(50 * time.Millisecond)

	if _, found, err := namespace.Get("default-ttl"); err != nil {
		t.Fatalf("failed to get default-TTL value: %v", err)
	} else if found {
		t.Fatal("ttl == 0 value did not expire using the backend default TTL")
	}
	if _, found, err := namespace.Get("expiring"); err != nil {
		t.Fatalf("failed to get expiring value: %v", err)
	} else if found {
		t.Fatal("positive TTL value did not expire")
	}
}

func TestMemoryNamespaceDefaultTTL(t *testing.T) {
	config := NewDefaultMemoryBackendConfig()
	config.DefaultTTL = 10 * time.Millisecond
	backend := NewMemoryCacheBackend(config)
	defer backend.Close()

	testCacheNamespaceDefaultTTL(t, backend.GetNamespace("ttl-test"), func() {})
}

func TestRistrettoNamespaceDefaultTTL(t *testing.T) {
	config := NewDefaultRistrettoBackendConfig()
	config.DefaultTTL = 10 * time.Millisecond
	backend, err := NewRistrettoCacheBackend(config)
	if err != nil {
		t.Fatalf("failed to create ristretto backend: %v", err)
	}
	defer backend.Close()

	namespace := backend.GetNamespace("ttl-test")
	ristrettoNamespace, ok := namespace.(*RistrettoNamespace)
	if !ok {
		t.Fatalf("namespace type = %T, want *RistrettoNamespace", namespace)
	}
	testCacheNamespaceDefaultTTL(t, namespace, ristrettoNamespace.cache.Wait)
}
