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

	logger       *log.Logger
	cacheFactory func(config *CacheConfig, accountID string) *FileSystemCache
}

var (
	cacheManager     *CacheManager
	cacheManagerOnce sync.Once
)

// GetCacheManager returns the singleton CacheManager instance
func GetCacheManager() *CacheManager {
	cacheManagerOnce.Do(func() {
		cacheManager = &CacheManager{
			caches:       make(map[string]*FileSystemCache),
			refCounts:    make(map[string]int),
			logger:       log.StandardLogger(),
			cacheFactory: NewFileSystemCache,
		}
	})
	return cacheManager
}

// AcquireCache gets or creates a cache for the given accountID
func (cm *CacheManager) AcquireCache(config *CacheConfig, accountID string) *FileSystemCache {
	cm.mu.Lock()

	logger := cm.logger.WithFields(log.Fields{
		"accountID": accountID,
	})

	// Check if cache already exists
	if cache, exists := cm.caches[accountID]; exists {
		cm.refCounts[accountID]++
		logger.WithFields(log.Fields{
			"refCount": cm.refCounts[accountID],
		}).Debug("reusing existing cache")
		cm.mu.Unlock()
		return cache
	}
	factory := cm.cacheFactory
	cm.mu.Unlock()

	// Backend initialization may perform network I/O (for example Redis Ping), so do it without
	// holding the manager-wide lock.
	if factory == nil {
		factory = NewFileSystemCache
	}
	cache := factory(config, accountID)

	cm.mu.Lock()
	if existingCache, exists := cm.caches[accountID]; exists {
		cm.refCounts[accountID]++
		refCount := cm.refCounts[accountID]
		cm.mu.Unlock()

		// Another goroutine installed a cache while this one was being created.
		cache.Close()
		logger.WithField("refCount", refCount).Debug("reusing concurrently created cache")
		return existingCache
	}
	cm.caches[accountID] = cache
	cm.refCounts[accountID] = 1
	cm.mu.Unlock()

	logger.Debug("created new shared cache")

	return cache
}

// ReleaseCache decrements the reference count and removes the cache if it reaches 0
func (cm *CacheManager) ReleaseCache(accountID string) {
	cm.mu.Lock()

	logger := cm.logger.WithFields(log.Fields{
		"accountID": accountID,
	})

	refCount, exists := cm.refCounts[accountID]
	if !exists {
		cm.mu.Unlock()
		logger.Warn("attempting to release non-existent cache")
		return
	}

	refCount--
	if refCount <= 0 {
		cache, _ := cm.caches[accountID]
		delete(cm.caches, accountID)
		delete(cm.refCounts, accountID)
		cm.mu.Unlock()

		if cache != nil {
			cache.Close()
		}
		logger.Debug("removed and closed cache (ref count reached 0)")
	} else {
		cm.refCounts[accountID] = refCount
		cm.mu.Unlock()
		logger.WithFields(log.Fields{
			"refCount": refCount,
		}).Debug("released cache reference")
	}
}

// Clear clears all caches (useful for testing)
func (cm *CacheManager) Clear() {
	cm.mu.Lock()
	caches := make([]*FileSystemCache, 0, len(cm.caches))
	for _, cache := range cm.caches {
		if cache != nil {
			caches = append(caches, cache)
		}
	}
	cm.caches = make(map[string]*FileSystemCache)
	cm.refCounts = make(map[string]int)
	cm.mu.Unlock()

	for _, cache := range caches {
		cache.Close()
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
