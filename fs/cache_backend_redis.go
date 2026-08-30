package fs

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
)

// RedisBackendConfig configures the Redis backend
type RedisBackendConfig struct {
	// Redis server address (host:port)
	Address string `yaml:"address,omitempty" json:"address,omitempty"`

	// Database number (0-15)
	DB int `yaml:"db,omitempty" json:"db,omitempty"`

	// Optional authentication password
	Password string `yaml:"password,omitempty" json:"password,omitempty"`

	// Connection pool size
	PoolSize int `yaml:"pool_size,omitempty" json:"pool_size,omitempty"`

	// Key prefix for all cache entries (useful for multi-tenant scenarios)
	KeyPrefix string `yaml:"key_prefix,omitempty" json:"key_prefix,omitempty"`

	// Connection timeout
	ConnectTimeout time.Duration `yaml:"connect_timeout,omitempty" json:"connect_timeout,omitempty"`

	// Command timeout
	CommandTimeout time.Duration `yaml:"command_timeout,omitempty" json:"command_timeout,omitempty"`

	// Default TTL for cache entries
	DefaultTTL time.Duration `yaml:"default_ttl,omitempty" json:"default_ttl,omitempty"`

	// Enable account-based namespace isolation for security
	// When true, cache keys are automatically prefixed with "account|zone"
	// This prevents different iRODS accounts from accessing each other's cached data
	// Recommended: true (for security), false (for performance if only single account)
	EnableAccountIsolation bool `yaml:"enable_account_isolation,omitempty" json:"enable_account_isolation,omitempty"`
}

// NewDefaultRedisBackendConfig creates a default redis backend configuration
func NewDefaultRedisBackendConfig() *RedisBackendConfig {
	return &RedisBackendConfig{
		Address:        "localhost:6379",
		DB:             0,
		PoolSize:       FileSystemCachePoolSize,
		ConnectTimeout: FileSystemCacheConnectTimeout,
		CommandTimeout: FileSystemCacheCommandTimeout,
		DefaultTTL:     FileSystemCacheTimeout,
	}
}

func (c *RedisBackendConfig) Validate() error {
	if c.Address == "" {
		return errors.New("redis address must not be empty")
	}

	if c.DB < 0 || c.DB > 15 {
		return errors.New("redis db must be between 0 and 15")
	}

	if c.PoolSize <= 0 {
		return errors.New("redis pool size must be positive")
	}

	if c.ConnectTimeout <= 0 {
		return errors.New("redis connect timeout must be positive")
	}

	if c.CommandTimeout <= 0 {
		return errors.New("redis command timeout must be positive")
	}

	return nil
}

// fillRedisBackendDefaults fills missing values with defaults
func fillRedisBackendDefaults(cfg *RedisBackendConfig) *RedisBackendConfig {
	if cfg.Address == "" {
		cfg.Address = "localhost:6379"
	}
	if cfg.DB < 0 {
		cfg.DB = 0
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = FileSystemCachePoolSize
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = FileSystemCacheConnectTimeout
	}
	if cfg.CommandTimeout <= 0 {
		cfg.CommandTimeout = FileSystemCacheCommandTimeout
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = FileSystemCacheTimeout
	}
	return cfg
}

// RedisCacheBackend implements CacheBackend using Redis
// This allows distributed caching across multiple instances
type RedisCacheBackend struct {
	client     *redis.Client
	config     *RedisBackendConfig
	defaultTTL time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	namespaces map[string]*RedisNamespace
	mu         sync.RWMutex
}

// NewRedisCacheBackend creates a new Redis cache backend
func NewRedisCacheBackend(config *RedisBackendConfig) (*RedisCacheBackend, error) {
	if config == nil {
		config = NewDefaultRedisBackendConfig()
	}

	defaultTTL := config.DefaultTTL
	if defaultTTL <= 0 {
		defaultTTL = FileSystemCacheTimeout
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:         config.Address,
		DB:           config.DB,
		Password:     config.Password,
		PoolSize:     config.PoolSize,
		ReadTimeout:  config.CommandTimeout,
		WriteTimeout: config.CommandTimeout,
		DialTimeout:  config.ConnectTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, errors.Wrap(err, "failed to connect to redis")
	}

	ctx, cancel = context.WithCancel(context.Background())

	return &RedisCacheBackend{
		client:     redisClient,
		config:     config,
		defaultTTL: defaultTTL,
		ctx:        ctx,
		cancel:     cancel,
		namespaces: make(map[string]*RedisNamespace),
	}, nil
}

// makeKey creates a prefixed key for Redis
func (r *RedisCacheBackend) makeKey(key string) string {
	if r.config.KeyPrefix != "" {
		return r.config.KeyPrefix + ":" + key
	}
	return key
}

// GetNamespace returns a namespace interface for isolated storage
// WARNING: Use GetNamespaceForAccount() for multi-account safety!
func (r *RedisCacheBackend) GetNamespace(namespace string) (CacheNamespace, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ns, ok := r.namespaces[namespace]; ok {
		return ns, nil
	}

	ns := &RedisNamespace{
		backend:         r,
		namespacePrefix: namespace + ":",
		defaultTTL:      r.defaultTTL,
	}

	r.namespaces[namespace] = ns
	return ns, nil
}

// DeleteNamespace removes all entries in a namespace
func (r *RedisCacheBackend) DeleteNamespace(namespace string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithTimeout(r.ctx, r.config.CommandTimeout)
	defer cancel()

	fullPrefix := r.makeKey(namespace + ":")
	pattern := fullPrefix + "*"

	keys := []string{}
	// Use SCAN with count=100 for pagination
	iter := r.client.Scan(ctx, 0, pattern, 100).Iterator()

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "failed to scan redis keys")
	}

	if len(keys) > 0 {
		if err := r.client.Del(ctx, keys...).Err(); err != nil {
			return errors.Wrap(err, "failed to delete keys")
		}
	}

	delete(r.namespaces, namespace)
	return nil
}

// Clear removes only entries owned by this cache backend.
func (r *RedisCacheBackend) Clear() error {
	r.mu.RLock()
	prefixes := r.clearPrefixes()
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(r.ctx, r.config.CommandTimeout)
	defer cancel()

	for _, prefix := range prefixes {
		if err := r.deleteKeysWithPrefix(ctx, prefix); err != nil {
			return err
		}
	}
	return nil
}

// clearPrefixes returns Redis key prefixes owned by this backend. The caller must hold r.mu.
func (r *RedisCacheBackend) clearPrefixes() []string {
	if r.config.KeyPrefix != "" {
		return []string{r.makeKey("")}
	}

	prefixes := make([]string, 0, len(r.namespaces))
	for namespace := range r.namespaces {
		prefixes = append(prefixes, namespace+":")
	}
	sort.Strings(prefixes)
	return prefixes
}

func (r *RedisCacheBackend) deleteKeysWithPrefix(ctx context.Context, prefix string) error {
	pattern := prefix + "*"
	iter := r.client.Scan(ctx, 0, pattern, 100).Iterator()
	keys := make([]string, 0)
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "failed to scan redis keys")
	}
	if len(keys) == 0 {
		return nil
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		return errors.Wrap(err, "failed to delete redis keys")
	}
	return nil
}

// Close closes the Redis connection
func (r *RedisCacheBackend) Close() error {
	r.cancel()
	return r.client.Close()
}

// RedisNamespace represents an isolated cache space for Redis
type RedisNamespace struct {
	backend         *RedisCacheBackend
	namespacePrefix string
	defaultTTL      time.Duration
}

// Get retrieves a namespaced value
func (rn *RedisNamespace) Get(key string) (interface{}, bool, error) {
	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	prefixedKey := rn.backend.makeKey(rn.namespacePrefix + key)

	data, err := rn.backend.client.Get(ctx, prefixedKey).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to get value from redis")
	}

	// Deserialize JSON
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, false, errors.Wrap(err, "failed to unmarshal value")
	}

	return value, true, nil
}

// Set stores a namespaced value with optional TTL
func (rn *RedisNamespace) Set(key string, value interface{}, ttl time.Duration) error {
	if ttl < 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	// Serialize value to JSON
	data, err := json.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "failed to marshal value")
	}

	prefixedKey := rn.backend.makeKey(rn.namespacePrefix + key)

	if ttl == 0 {
		ttl = rn.defaultTTL
	}

	return rn.backend.client.Set(ctx, prefixedKey, data, ttl).Err()
}

// Delete removes a namespaced key
func (rn *RedisNamespace) Delete(key string) error {
	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	prefixedKey := rn.backend.makeKey(rn.namespacePrefix + key)

	return rn.backend.client.Del(ctx, prefixedKey).Err()
}

// Exists checks if a namespaced key exists
func (rn *RedisNamespace) Exists(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	prefixedKey := rn.backend.makeKey(rn.namespacePrefix + key)

	exists, err := rn.backend.client.Exists(ctx, prefixedKey).Result()
	if err != nil {
		return false, errors.Wrap(err, "failed to check existence in redis")
	}

	return exists > 0, nil
}

// DeletePrefix removes all keys with a given prefix within this namespace
func (rn *RedisNamespace) DeletePrefix(prefix string) error {
	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	fullPrefix := rn.backend.makeKey(rn.namespacePrefix + prefix)
	pattern := fullPrefix + "*"

	keys := []string{}
	// Use SCAN with count=100 for pagination
	iter := rn.backend.client.Scan(ctx, 0, pattern, 100).Iterator()

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "failed to scan redis keys")
	}

	if len(keys) > 0 {
		return rn.backend.client.Del(ctx, keys...).Err()
	}

	return nil
}

// GetPrefix retrieves all keys and values with a given prefix within this namespace
func (rn *RedisNamespace) GetPrefix(prefix string) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(rn.backend.ctx, rn.backend.config.CommandTimeout)
	defer cancel()

	fullPrefix := rn.backend.makeKey(rn.namespacePrefix + prefix)
	pattern := fullPrefix + "*"

	result := make(map[string]interface{})

	// Use SCAN with count=100 for pagination
	iter := rn.backend.client.Scan(ctx, 0, pattern, 100).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		data, err := rn.backend.client.Get(ctx, key).Bytes()
		if err != nil && err != redis.Nil {
			return nil, errors.Wrap(err, "failed to get value from redis")
		}

		if err == nil {
			var value interface{}
			if err := json.Unmarshal(data, &value); err != nil {
				continue
			}
			result[key] = value
		}
	}

	if err := iter.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to scan redis keys")
	}

	return result, nil
}

// Clear removes all entries in this namespace
func (rn *RedisNamespace) Clear() error {
	return rn.DeletePrefix("")
}
