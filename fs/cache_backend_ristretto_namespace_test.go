package fs

import (
	"strings"
	"testing"
)

func TestRistrettoGetNamespaceReturnsCreationError(t *testing.T) {
	backend, err := NewRistrettoCacheBackend(nil)
	if err != nil {
		t.Fatalf("NewRistrettoCacheBackend() error = %v", err)
	}

	// Simulate an invalid configuration reaching the deferred namespace creation.
	backend.config.MaxEntries = 0
	namespace, namespaceErr := backend.GetNamespace("invalid")
	if namespace != nil {
		t.Fatal("GetNamespace() returned a namespace for invalid configuration")
	}
	if namespaceErr == nil || !strings.Contains(namespaceErr.Error(), "NumCounters can't be zero") {
		t.Fatalf("GetNamespace() error = %v, want ristretto creation error", namespaceErr)
	}
}
