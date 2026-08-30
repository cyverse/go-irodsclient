package fs

import "testing"

func testDeletePrefixRemovesEveryKey(t *testing.T, namespace CacheNamespace, waitForWrites func()) {
	t.Helper()

	keys := []string{"/dir/a", "/dir/b", "/dir/c"}
	for _, key := range keys {
		if err := namespace.Set(key, key, 0); err != nil {
			t.Fatalf("failed to set %q: %v", key, err)
		}
	}
	waitForWrites()

	if err := namespace.DeletePrefix("/dir/"); err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}
	waitForWrites()

	for _, key := range keys {
		if _, found, err := namespace.Get(key); err != nil {
			t.Fatalf("failed to get %q: %v", key, err)
		} else if found {
			t.Errorf("key %q remains after DeletePrefix", key)
		}
	}

	remaining, err := namespace.GetPrefix("/dir/")
	if err != nil {
		t.Fatalf("GetPrefix failed: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("prefix index retains keys after deletion: %v", remaining)
	}
}

func TestMemoryNamespaceDeletePrefixRemovesEveryKey(t *testing.T) {
	backend := NewMemoryCacheBackend(NewDefaultMemoryBackendConfig())
	defer backend.Close()

	testDeletePrefixRemovesEveryKey(t, backend.GetNamespace("test"), func() {})
}

func TestRistrettoNamespaceDeletePrefixRemovesEveryKey(t *testing.T) {
	backend, err := NewRistrettoCacheBackend(NewDefaultRistrettoBackendConfig())
	if err != nil {
		t.Fatalf("failed to create ristretto backend: %v", err)
	}
	defer backend.Close()

	namespace := backend.GetNamespace("test")
	ristrettoNamespace, ok := namespace.(*RistrettoNamespace)
	if !ok {
		t.Fatalf("namespace type = %T, want *RistrettoNamespace", namespace)
	}
	testDeletePrefixRemovesEveryKey(t, namespace, ristrettoNamespace.cache.Wait)
}
