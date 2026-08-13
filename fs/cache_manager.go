package fs

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// CacheManager manages shared caches for FileSystem instances
// Instances with the same accountID share the same cache
type CacheManager struct {
	mu sync.RWMutex

	// map of accountID -> cache instance
	caches map[string]*FileSystemCache

	// map of accountID -> reference count
	refCounts map[string]int

	logger *log.Logger
}

var (
	cacheManager     *CacheManager
	cacheManagerOnce sync.Once
)

// GetCacheManager returns the singleton CacheManager instance
func GetCacheManager() *CacheManager {
	cacheManagerOnce.Do(func() {
		cacheManager = &CacheManager{
			caches:    make(map[string]*FileSystemCache),
			refCounts: make(map[string]int),
			logger:    log.StandardLogger(),
		}
	})
	return cacheManager
}

// AcquireCache gets or creates a cache for the given accountID
func (cm *CacheManager) AcquireCache(config *CacheConfig, accountID string) *FileSystemCache {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	logger := cm.logger.WithFields(log.Fields{
		"accountID": accountID,
	})

	// Check if cache already exists
	if cache, exists := cm.caches[accountID]; exists {
		cm.refCounts[accountID]++
		logger.WithFields(log.Fields{
			"refCount": cm.refCounts[accountID],
		}).Debug("reusing existing cache")
		return cache
	}

	// Create new cache
	cache := NewFileSystemCache(config, accountID)
	cm.caches[accountID] = cache
	cm.refCounts[accountID] = 1

	logger.Debug("created new shared cache")

	return cache
}

// ReleaseCache decrements the reference count and removes the cache if it reaches 0
func (cm *CacheManager) ReleaseCache(accountID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	logger := cm.logger.WithFields(log.Fields{
		"accountID": accountID,
	})

	refCount, exists := cm.refCounts[accountID]
	if !exists {
		logger.Warn("attempting to release non-existent cache")
		return
	}

	refCount--
	if refCount <= 0 {
		cache, _ := cm.caches[accountID]
		if cache != nil {
			cache.Close()
		}
		delete(cm.caches, accountID)
		delete(cm.refCounts, accountID)

		logger.Debug("removed and closed cache (ref count reached 0)")
	} else {
		cm.refCounts[accountID] = refCount
		logger.WithFields(log.Fields{
			"refCount": refCount,
		}).Debug("released cache reference")
	}
}

// Clear clears all caches (useful for testing)
func (cm *CacheManager) Clear() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for accountID, cache := range cm.caches {
		if cache != nil {
			cache.Close()
		}
		delete(cm.caches, accountID)
		delete(cm.refCounts, accountID)
	}
}

// GetStats returns cache statistics for debugging
func (cm *CacheManager) GetStats() map[string]int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := make(map[string]int)
	for accountID, refCount := range cm.refCounts {
		stats[accountID] = refCount
	}
	return stats
}
