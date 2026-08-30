package fs

import (
	"reflect"
	"testing"
)

func TestRedisClearPrefixesUsesConfiguredKeyPrefix(t *testing.T) {
	backend := &RedisCacheBackend{
		config: &RedisBackendConfig{KeyPrefix: "irods-cache"},
		namespaces: map[string]*RedisNamespace{
			"account:entries": {},
		},
	}

	got := backend.clearPrefixes()
	want := []string{"irods-cache:"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clear prefixes = %v, want %v", got, want)
	}
}

func TestRedisClearPrefixesUsesKnownNamespacesWithoutKeyPrefix(t *testing.T) {
	backend := &RedisCacheBackend{
		config: &RedisBackendConfig{},
		namespaces: map[string]*RedisNamespace{
			"account:users":   {},
			"account:entries": {},
		},
	}

	got := backend.clearPrefixes()
	want := []string{"account:entries:", "account:users:"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clear prefixes = %v, want %v", got, want)
	}
}

func TestRedisClearPrefixesNeverFallsBackToWholeDatabase(t *testing.T) {
	backend := &RedisCacheBackend{
		config:     &RedisBackendConfig{},
		namespaces: map[string]*RedisNamespace{},
	}

	if prefixes := backend.clearPrefixes(); len(prefixes) != 0 {
		t.Fatalf("empty backend clear prefixes = %v, want no whole-database pattern", prefixes)
	}
}
