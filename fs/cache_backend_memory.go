package fs

import (
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	gocache "github.com/patrickmn/go-cache"
)

// MemoryBackendConfig configures the in-memory go-cache backend
type MemoryBackendConfig struct {
	// Cleanup interval for expired entries
	CleanupInterval time.Duration `yaml:"cleanup_interval,omitempty" json:"cleanup_interval,omitempty"`
	// Default TTL for cache entries
	DefaultTTL time.Duration `yaml:"default_ttl,omitempty" json:"default_ttl,omitempty"`
}

// NewDefaultMemoryBackendConfig creates a default memory backend configuration
func NewDefaultMemoryBackendConfig() *MemoryBackendConfig {
	return &MemoryBackendConfig{
		CleanupInterval: FileSystemCacheTimeout,
		DefaultTTL:      FileSystemCacheTimeout,
	}
}

func (c *MemoryBackendConfig) Validate() error {
	if c.CleanupInterval <= 0 {
		return errors.New("memory backend cleanup interval must be positive")
	}

	return nil
}

// fillMemoryBackendDefaults fills missing values with defaults
func fillMemoryBackendDefaults(cfg *MemoryBackendConfig) *MemoryBackendConfig {
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = FileSystemCacheTimeout
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = FileSystemCacheTimeout
	}
	return cfg
}

// MemoryCacheBackend implements CacheBackend using go-cache
// Each namespace has its own independent cache
type MemoryCacheBackend struct {
	cleanupInterval time.Duration
	defaultTTL      time.Duration
	caches          map[string]*gocache.Cache
	namespaces      map[string]*MemoryNamespace
	mu              sync.RWMutex
}

// NewMemoryCacheBackend creates a new in-memory cache backend with config
func NewMemoryCacheBackend(config *MemoryBackendConfig) *MemoryCacheBackend {
	cleanupInterval := config.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = FileSystemCacheCleanupInterval
	}

	defaultTTL := config.DefaultTTL
	if defaultTTL <= 0 {
		defaultTTL = FileSystemCacheTimeout
	}

	return &MemoryCacheBackend{
		cleanupInterval: cleanupInterval,
		defaultTTL:      defaultTTL,
		caches:          make(map[string]*gocache.Cache),
		namespaces:      make(map[string]*MemoryNamespace),
	}
}

// GetNamespace returns a namespace interface for isolated storage
// WARNING: Use GetNamespaceForAccount() for multi-account safety!
func (m *MemoryCacheBackend) GetNamespace(namespace string) CacheNamespace {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return existing namespace
	if ns, ok := m.namespaces[namespace]; ok {
		return ns
	}

	// Create a new cache for this namespace
	cache := gocache.New(m.defaultTTL, m.cleanupInterval)
	m.caches[namespace] = cache

	ns := &MemoryNamespace{
		cache:       cache,
		prefixIndex: make(map[string][]string),
	}
	m.namespaces[namespace] = ns

	return ns
}

// DeleteNamespace removes all entries in a namespace
func (m *MemoryCacheBackend) DeleteNamespace(namespace string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cache, ok := m.caches[namespace]; ok {
		cache.Flush()
		delete(m.caches, namespace)
	}

	delete(m.namespaces, namespace)
	return nil
}

// Clear removes all entries from the cache
func (m *MemoryCacheBackend) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cache := range m.caches {
		cache.Flush()
	}
	m.caches = make(map[string]*gocache.Cache)
	m.namespaces = make(map[string]*MemoryNamespace)
	return nil
}

// Close closes the backend (no-op for in-memory backend)
func (m *MemoryCacheBackend) Close() error {
	return m.Clear()
}

// MemoryNamespace represents an isolated cache space
type MemoryNamespace struct {
	cache       *gocache.Cache
	prefixIndex map[string][]string // prefix → [keys]
	mu          sync.RWMutex
}

// Get retrieves a value
func (mn *MemoryNamespace) Get(key string) (interface{}, bool, error) {
	value, found := mn.cache.Get(key)
	return value, found, nil
}

// Set stores a value
func (mn *MemoryNamespace) Set(key string, value interface{}, ttl time.Duration) error {
	if ttl < 0 {
		return nil
	}

	mn.mu.Lock()
	defer mn.mu.Unlock()

	// if ttl is 0, use default
	mn.cache.Set(key, value, ttl)

	// Register all prefixes for this key in prefixIndex
	mn.addKeyToPrefixes(key)
	return nil
}

// addKeyToPrefixes registers key in all relevant prefixes
func (mn *MemoryNamespace) addKeyToPrefixes(key string) {
	// Generate all prefixes for this key
	prefixes := mn.generatePrefixes(key)
	for _, prefix := range prefixes {
		// Check if key already exists in this prefix's list
		exists := false
		for _, k := range mn.prefixIndex[prefix] {
			if k == key {
				exists = true
				break
			}
		}
		if !exists {
			mn.prefixIndex[prefix] = append(mn.prefixIndex[prefix], key)
		}
	}
}

// generatePrefixes generates all prefixes for a given key
func (mn *MemoryNamespace) generatePrefixes(key string) []string {
	var prefixes []string
	current := ""

	for i, ch := range key {
		current += string(ch)
		if ch == '/' || i == len(key)-1 {
			prefixes = append(prefixes, current)
		}
	}

	return prefixes
}

// Delete removes a key
func (mn *MemoryNamespace) Delete(key string) error {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	mn.cache.Delete(key)
	mn.removeKeyFromPrefixes(key)
	return nil
}

// removeKeyFromPrefixes removes key from all relevant prefixes
func (mn *MemoryNamespace) removeKeyFromPrefixes(key string) {
	prefixes := mn.generatePrefixes(key)
	for _, prefix := range prefixes {
		keys := mn.prefixIndex[prefix]
		for i, k := range keys {
			if k == key {
				mn.prefixIndex[prefix] = append(keys[:i], keys[i+1:]...)
				break
			}
		}
		if len(mn.prefixIndex[prefix]) == 0 {
			delete(mn.prefixIndex, prefix)
		}
	}
}

// Exists checks if a key exists
func (mn *MemoryNamespace) Exists(key string) (bool, error) {
	_, found := mn.cache.Get(key)
	return found, nil
}

// DeletePrefix removes all keys with a given prefix
func (mn *MemoryNamespace) DeletePrefix(prefix string) error {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	keys := mn.prefixIndex[prefix]
	for _, k := range keys {
		mn.cache.Delete(k)
		mn.removeKeyFromPrefixes(k)
	}
	delete(mn.prefixIndex, prefix)
	return nil
}

// GetPrefix retrieves all keys and values with a given prefix
// Uses lazy cleanup to remove expired entries
func (mn *MemoryNamespace) GetPrefix(prefix string) (map[string]interface{}, error) {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	result := make(map[string]interface{})
	keys := mn.prefixIndex[prefix]
	keysToRemove := []string{}

	for _, key := range keys {
		value, found := mn.cache.Get(key)
		if found {
			result[key] = value
		} else {
			// TTL expired, mark for removal
			keysToRemove = append(keysToRemove, key)
		}
	}

	// Lazy cleanup: remove expired entries
	for _, key := range keysToRemove {
		mn.removeKeyFromPrefixes(key)
	}

	return result, nil
}

// Clear removes all entries in this namespace
func (mn *MemoryNamespace) Clear() error {
	mn.mu.Lock()
	defer mn.mu.Unlock()

	mn.cache.Flush()
	mn.prefixIndex = make(map[string][]string)
	return nil
}
