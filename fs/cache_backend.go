package fs

import (
	"time"

	"github.com/cockroachdb/errors"
)

// CacheBackendType defines supported cache backend types
type CacheBackendType string

const (
	// CacheBackendTypeMemory uses in-memory go-cache backend
	CacheBackendTypeMemory CacheBackendType = "memory"
	// CacheBackendTypeRistretto uses ristretto backend
	CacheBackendTypeRistretto CacheBackendType = "ristretto"
	// CacheBackendTypeRedis uses external redis server backend
	CacheBackendTypeRedis CacheBackendType = "redis"
	// CacheBackendTypeNone disables caching
	CacheBackendTypeNone CacheBackendType = "none"
)

// CacheItem represents a single cache entry
type CacheItem struct {
	Key       string
	Value     interface{}
	ExpiresAt time.Time
}

// CacheNamespace provides isolated cache storage
// Useful when different components need separate cache spaces
type CacheNamespace interface {
	// Get retrieves a namespaced value
	// Returns (value, found, error)
	Get(key string) (interface{}, bool, error)

	// Set stores a namespaced value with optional TTL
	// ttl == 0 means no expiration
	// ttl < 0 means no caching
	Set(key string, value interface{}, ttl time.Duration) error

	// Delete removes a namespaced key
	Delete(key string) error

	// Exists checks if a namespaced key exists
	Exists(key string) (bool, error)

	// DeletePrefix removes all keys with a given prefix within this namespace
	DeletePrefix(prefix string) error

	// GetPrefix retrieves all keys and values with a given prefix within this namespace
	GetPrefix(prefix string) (map[string]interface{}, error)

	// Clear removes all entries in this namespace
	Clear() error
}

// CacheBackend defines the interface for cache implementations
// Namespace support is built-in for all backends
//
// IMPORTANT: Multi-account safety
// Since multiple FileSystem instances can exist with different accounts,
// cache namespace IDs must be derived from host, account, and zone to prevent cross-account data leaks.
// The namespace prefix (e.g., "entries", "metadata") is controlled by the caller.
type CacheBackend interface {
	// GetNamespace returns a namespace interface for isolated storage
	// The namespace parameter should be pre-computed by the caller as: accountID + ":" + logicalNamespace
	// where accountID is a hash of (host, account, zone)
	// Examples:
	//   - "hash(host|account|zone):entries"
	//   - "hash(host|account|zone):metadata"
	GetNamespace(namespace string) CacheNamespace

	// DeleteNamespace removes all entries in a namespace
	DeleteNamespace(namespace string) error

	// Clear removes all entries from the entire cache
	Clear() error

	// Close closes the backend connection (useful for Redis)
	Close() error
}

// CacheBackendConfig holds configuration for all backend types
type CacheBackendConfig struct {
	// Backend type (memory, ristretto, redis)
	Type CacheBackendType `yaml:"type,omitempty" json:"type,omitempty"`

	// Default TTL for all cache entries
	DefaultTTL time.Duration `yaml:"default_ttl,omitempty" json:"default_ttl,omitempty"`

	// Memory backend configuration
	Memory *MemoryBackendConfig `yaml:"memory,omitempty" json:"memory,omitempty"`

	// Ristretto backend configuration
	Ristretto *RistrettoBackendConfig `yaml:"ristretto,omitempty" json:"ristretto,omitempty"`

	// Redis backend configuration
	Redis *RedisBackendConfig `yaml:"redis,omitempty" json:"redis,omitempty"`
}

// Validate checks if the configuration is valid
func (c *CacheBackendConfig) Validate() error {
	switch c.Type {
	case CacheBackendTypeMemory:
		if c.Memory == nil {
			return errors.New("memory cache backend config is nil")
		}
		return c.Memory.Validate()
	case CacheBackendTypeRistretto:
		if c.Ristretto == nil {
			return errors.New("ristretto cache backend config is nil")
		}
		return c.Ristretto.Validate()
	case CacheBackendTypeRedis:
		if c.Redis == nil {
			return errors.New("redis cache backend config is nil")
		}
		return c.Redis.Validate()
	case CacheBackendTypeNone:
		return nil
	default:
		return errors.Newf("unsupported cache backend type: %s", c.Type)
	}
}
