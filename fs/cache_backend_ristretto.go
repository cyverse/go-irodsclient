package fs

import (
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dgraph-io/ristretto"
)

// RistrettoBackendConfig configures the Ristretto backend
type RistrettoBackendConfig struct {
	// Maximum number of entries in the cache
	MaxEntries int64 `yaml:"max_entries,omitempty" json:"max_entries,omitempty"`

	// Cost per entry (used for eviction)
	// When total cost exceeds MaxCost, entries are evicted
	MaxCost int64 `yaml:"max_cost,omitempty" json:"max_cost,omitempty"`

	// Number of buffers for concurrent access
	BufferItems int64 `yaml:"buffer_items,omitempty" json:"buffer_items,omitempty"`

	// Default TTL for cache entries
	DefaultTTL time.Duration `yaml:"default_ttl,omitempty" json:"default_ttl,omitempty"`
}

// NewDefaultRistrettoBackendConfig creates a default ristretto backend configuration
func NewDefaultRistrettoBackendConfig() *RistrettoBackendConfig {
	return &RistrettoBackendConfig{
		MaxEntries:  FileSystemCacheMaxEntries,
		MaxCost:     FileSystemCacheMaxCost,
		BufferItems: FileSystemCacheBufferItems,
		DefaultTTL:  FileSystemCacheTimeout,
	}
}

func (c *RistrettoBackendConfig) Validate() error {
	if c.MaxEntries <= 0 {
		return errors.New("ristretto max entries must be positive")
	}

	if c.MaxCost <= 0 {
		return errors.New("ristretto max cost must be positive")
	}

	if c.BufferItems <= 0 {
		return errors.New("ristretto buffer items must be positive")
	}

	return nil
}

// fillRistrettoBackendDefaults fills missing values with defaults
func fillRistrettoBackendDefaults(cfg *RistrettoBackendConfig) *RistrettoBackendConfig {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = FileSystemCacheMaxEntries
	}
	if cfg.MaxCost <= 0 {
		cfg.MaxCost = FileSystemCacheMaxCost
	}
	if cfg.BufferItems <= 0 {
		cfg.BufferItems = FileSystemCacheBufferItems
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = FileSystemCacheTimeout
	}
	return cfg
}

// RistrettoCacheBackend implements CacheBackend using Ristretto
// Ristretto is a high-performance caching library optimized for speed and memory efficiency
// Each namespace has its own independent cache
type RistrettoCacheBackend struct {
	config     *RistrettoBackendConfig
	caches     map[string]*ristretto.Cache
	ttlMaps    map[string]map[string]time.Time
	namespaces map[string]*RistrettoNamespace
	mu         sync.RWMutex
}

// NewRistrettoCacheBackend creates a new Ristretto cache backend
func NewRistrettoCacheBackend(config *RistrettoBackendConfig) (*RistrettoCacheBackend, error) {
	if config == nil {
		config = NewDefaultRistrettoBackendConfig()
	}
	config = fillRistrettoBackendDefaults(config)

	return &RistrettoCacheBackend{
		config:     config,
		caches:     make(map[string]*ristretto.Cache),
		ttlMaps:    make(map[string]map[string]time.Time),
		namespaces: make(map[string]*RistrettoNamespace),
	}, nil
}

// GetNamespace returns a namespace interface for isolated storage
// WARNING: Use GetNamespaceForAccount() for multi-account safety!
func (r *RistrettoCacheBackend) GetNamespace(namespace string) CacheNamespace {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return existing namespace
	if ns, ok := r.namespaces[namespace]; ok {
		return ns
	}

	// Create a new cache for this namespace
	ristrettoConfig := &ristretto.Config{
		NumCounters: r.config.MaxEntries,
		MaxCost:     r.config.MaxCost,
		BufferItems: r.config.BufferItems,
	}

	cache, err := ristretto.NewCache(ristrettoConfig)
	if err != nil {
		return nil
	}

	ttlMap := make(map[string]time.Time)
	r.caches[namespace] = cache
	r.ttlMaps[namespace] = ttlMap

	ns := &RistrettoNamespace{
		cache:       cache,
		ttlMap:      ttlMap,
		prefixIndex: make(map[string][]string),
		defaultTTL:  r.config.DefaultTTL,
	}
	r.namespaces[namespace] = ns

	return ns
}

// DeleteNamespace removes all entries in a namespace
func (r *RistrettoCacheBackend) DeleteNamespace(namespace string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cache, ok := r.caches[namespace]; ok {
		cache.Close()
		delete(r.caches, namespace)
	}

	delete(r.ttlMaps, namespace)
	delete(r.namespaces, namespace)
	return nil
}

// Clear removes all entries from the cache
func (r *RistrettoCacheBackend) Clear() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, cache := range r.caches {
		cache.Close()
	}

	r.caches = make(map[string]*ristretto.Cache)
	r.ttlMaps = make(map[string]map[string]time.Time)
	r.namespaces = make(map[string]*RistrettoNamespace)
	return nil
}

// Close closes the backend
func (r *RistrettoCacheBackend) Close() error {
	return r.Clear()
}

// RistrettoNamespace represents an isolated cache space for Ristretto
type RistrettoNamespace struct {
	cache       *ristretto.Cache
	ttlMap      map[string]time.Time
	prefixIndex map[string][]string // prefix → [keys]
	defaultTTL  time.Duration
	mu          sync.RWMutex
}

// Get retrieves a namespaced value
func (rn *RistrettoNamespace) Get(key string) (interface{}, bool, error) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// Check if TTL expired
	if expiresAt, ok := rn.ttlMap[key]; ok {
		if time.Now().After(expiresAt) {
			// Expired, delete it
			rn.mu.RUnlock()
			_ = rn.Delete(key)
			rn.mu.RLock()
			return nil, false, nil
		}
	}

	value, found := rn.cache.Get(key)
	return value, found, nil
}

// Set stores a namespaced value
func (rn *RistrettoNamespace) Set(key string, value interface{}, ttl time.Duration) error {
	if ttl < 0 {
		return nil
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if ttl == 0 {
		ttl = rn.defaultTTL
	}

	rn.cache.Set(key, value, 1)
	rn.ttlMap[key] = time.Now().Add(ttl)

	// Register all prefixes for this key in prefixIndex
	rn.addKeyToPrefixes(key)

	return nil
}

// Delete removes a namespaced key
func (rn *RistrettoNamespace) Delete(key string) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.cache.Del(key)
	delete(rn.ttlMap, key)
	rn.removeKeyFromPrefixes(key)
	return nil
}

// Exists checks if a namespaced key exists
func (rn *RistrettoNamespace) Exists(key string) (bool, error) {
	_, found, _ := rn.Get(key)
	return found, nil
}

// DeletePrefix removes all keys with a given prefix within this namespace
func (rn *RistrettoNamespace) DeletePrefix(prefix string) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	keys := slices.Clone(rn.prefixIndex[prefix])
	for _, k := range keys {
		rn.cache.Del(k)
		delete(rn.ttlMap, k)
		rn.removeKeyFromPrefixes(k)
	}
	delete(rn.prefixIndex, prefix)
	return nil
}

// GetPrefix retrieves all keys and values with a given prefix within this namespace
// Uses lazy cleanup to remove expired entries
func (rn *RistrettoNamespace) GetPrefix(prefix string) (map[string]interface{}, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	result := make(map[string]interface{})
	keys := rn.prefixIndex[prefix]
	keysToRemove := []string{}

	for _, key := range keys {
		// Check if TTL expired
		if expiresAt, ok := rn.ttlMap[key]; ok {
			if time.Now().After(expiresAt) {
				// Expired, mark for removal
				keysToRemove = append(keysToRemove, key)
				continue
			}
		}

		// Get value from cache
		if v, found := rn.cache.Get(key); found {
			result[key] = v
		}
	}

	// Lazy cleanup: remove expired entries
	for _, key := range keysToRemove {
		rn.removeKeyFromPrefixes(key)
	}

	return result, nil
}

// Clear removes all entries in this namespace
func (rn *RistrettoNamespace) Clear() error {
	return rn.DeletePrefix("")
}

// addKeyToPrefixes registers key in all relevant prefixes
func (rn *RistrettoNamespace) addKeyToPrefixes(key string) {
	prefixes := rn.generatePrefixes(key)
	for _, prefix := range prefixes {
		exists := false
		for _, k := range rn.prefixIndex[prefix] {
			if k == key {
				exists = true
				break
			}
		}
		if !exists {
			rn.prefixIndex[prefix] = append(rn.prefixIndex[prefix], key)
		}
	}
}

// removeKeyFromPrefixes removes key from all relevant prefixes
func (rn *RistrettoNamespace) removeKeyFromPrefixes(key string) {
	prefixes := rn.generatePrefixes(key)
	for _, prefix := range prefixes {
		keys := rn.prefixIndex[prefix]
		for i, k := range keys {
			if k == key {
				rn.prefixIndex[prefix] = append(keys[:i], keys[i+1:]...)
				break
			}
		}
		if len(rn.prefixIndex[prefix]) == 0 {
			delete(rn.prefixIndex, prefix)
		}
	}
}

// generatePrefixes generates all prefixes for a given key
func (rn *RistrettoNamespace) generatePrefixes(key string) []string {
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
