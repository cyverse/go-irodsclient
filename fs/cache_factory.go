package fs

import (
	"github.com/cockroachdb/errors"
)

// CacheBackendFactory creates cache backend instances based on configuration
type CacheBackendFactory struct {
	config *CacheBackendConfig
}

// NewCacheBackendFactory creates a new cache backend factory
func NewCacheBackendFactory(config *CacheBackendConfig) *CacheBackendFactory {
	if config == nil {
		config = NewDefaultCacheBackendConfig()
	}
	return &CacheBackendFactory{
		config: config,
	}
}

// NewDefaultCacheBackendConfig creates a default cache backend configuration
// Note: Backend-specific defaults are defined in their respective files
func NewDefaultCacheBackendConfig() *CacheBackendConfig {
	return &CacheBackendConfig{
		Type:      CacheBackendTypeMemory,
		Memory:    NewDefaultMemoryBackendConfig(),
		Ristretto: NewDefaultRistrettoBackendConfig(),
		Redis:     NewDefaultRedisBackendConfig(),
	}
}

// CreateBackend creates a cache backend instance based on the configured type
func (f *CacheBackendFactory) CreateBackend() (CacheBackend, error) {
	switch f.config.Type {
	case CacheBackendTypeMemory:
		return f.createMemoryBackend()
	case CacheBackendTypeRistretto:
		return f.createRistrettoBackend()
	case CacheBackendTypeRedis:
		return f.createRedisBackend()
	case CacheBackendTypeNone:
		return f.createNoneBackend()
	default:
		return nil, errors.Newf("unsupported cache backend type: %s", f.config.Type)
	}
}

func (f *CacheBackendFactory) createMemoryBackend() (CacheBackend, error) {
	cfg := f.config.Memory
	if cfg == nil {
		cfg = NewDefaultMemoryBackendConfig()
	} else {
		cfg = fillMemoryBackendDefaults(cfg)
	}
	return NewMemoryCacheBackend(cfg), nil
}

func (f *CacheBackendFactory) createRistrettoBackend() (CacheBackend, error) {
	cfg := f.config.Ristretto
	if cfg == nil {
		cfg = NewDefaultRistrettoBackendConfig()
	} else {
		cfg = fillRistrettoBackendDefaults(cfg)
	}
	return NewRistrettoCacheBackend(cfg)
}

func (f *CacheBackendFactory) createRedisBackend() (CacheBackend, error) {
	cfg := f.config.Redis
	if cfg == nil {
		cfg = NewDefaultRedisBackendConfig()
	} else {
		cfg = fillRedisBackendDefaults(cfg)
	}
	return NewRedisCacheBackend(cfg)
}

func (f *CacheBackendFactory) createNoneBackend() (CacheBackend, error) {
	return NewNoCacheBackend(), nil
}
