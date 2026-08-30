package fs

import (
	"strconv"
	"testing"
)

func TestRistrettoSetIsVisibleBeforeReturn(t *testing.T) {
	backend, err := NewRistrettoCacheBackend(NewDefaultRistrettoBackendConfig())
	if err != nil {
		t.Fatalf("failed to create ristretto backend: %v", err)
	}
	defer backend.Close()

	namespace, err := backend.GetNamespace("set-visible-test")
	if err != nil {
		t.Fatalf("GetNamespace() error = %v", err)
	}
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		if err := namespace.Set(key, i, 0); err != nil {
			t.Fatalf("Set(%d) failed: %v", i, err)
		}

		value, found, err := namespace.Get(key)
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", i, err)
		}
		if !found {
			t.Fatalf("key %d was not visible immediately after Set returned", i)
		}
		if value != i {
			t.Fatalf("value for key %d = %v, want %d", i, value, i)
		}
	}
}
