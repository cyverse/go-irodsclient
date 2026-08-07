package fs

import (
	"fmt"
	"sync"
)

// CacheMetrics collects cache statistics
type CacheMetrics struct {
	Hits    int64
	Misses  int64
	Sets    int64
	Deletes int64
	mu      sync.RWMutex
}

// IncrementHits increments hit counter
func (cm *CacheMetrics) IncrementHits() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Hits++
}

// IncrementMisses increments miss counter
func (cm *CacheMetrics) IncrementMisses() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Misses++
}

// IncrementSets increments set counter
func (cm *CacheMetrics) IncrementSets() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Sets++
}

// IncrementDeletes increments delete counter
func (cm *CacheMetrics) IncrementDeletes() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Deletes++
}

// GetHitRate returns the hit rate as a percentage
func (cm *CacheMetrics) GetHitRate() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	total := cm.Hits + cm.Misses
	if total == 0 {
		return 0
	}
	return float64(cm.Hits) / float64(total) * 100
}

// String returns a string representation of the metrics
func (cm *CacheMetrics) String() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	total := cm.Hits + cm.Misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(cm.Hits) / float64(total) * 100
	}

	return fmt.Sprintf("Hits: %d, Misses: %d, Sets: %d, Deletes: %d, HitRate: %.2f%%",
		cm.Hits, cm.Misses, cm.Sets, cm.Deletes, hitRate)
}
