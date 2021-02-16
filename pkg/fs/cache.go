package fs

import (
	"fmt"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/util"
	gocache "github.com/patrickmn/go-cache"
)

// FileSystemCache ...
type FileSystemCache struct {
	CacheTimeout   time.Duration
	CleanupTimeout time.Duration
	EntryCache     *gocache.Cache
	DirCache       *gocache.Cache
}

// NewFileSystemCache creates a new FileSystemCache
func NewFileSystemCache(cacheTimeout time.Duration, cleanup time.Duration) *FileSystemCache {
	entryCache := gocache.New(cacheTimeout, cleanup)
	dirCache := gocache.New(cacheTimeout, cleanup)

	return &FileSystemCache{
		CacheTimeout:   cacheTimeout,
		CleanupTimeout: cleanup,
		EntryCache:     entryCache,
		DirCache:       dirCache,
	}
}

func shouldHaveInfiniteCacheTTL(path string) bool {
	zone, err := util.GetIRODSZone(path)
	if err != nil {
		return false
	}

	root := "/"
	zoneRoot := fmt.Sprintf("/%s", zone)
	home := fmt.Sprintf("/%s/home", zone)

	switch path {
	case root:
		return true
	case zoneRoot:
		return true
	case home:
		return true
	default:
		return false
	}
}

// AddEntryCache ...
func (cache *FileSystemCache) AddEntryCache(entry *FSEntry) {
	if shouldHaveInfiniteCacheTTL(entry.Path) {
		cache.EntryCache.Set(entry.Path, entry, -1)
	}

	// default
	cache.EntryCache.Set(entry.Path, entry, 0)
}

// RemoveEntryCache ...
func (cache *FileSystemCache) RemoveEntryCache(path string) {
	cache.EntryCache.Delete(path)
}

// GetEntryCache ...
func (cache *FileSystemCache) GetEntryCache(path string) *FSEntry {
	entry, _ := cache.EntryCache.Get(path)
	if fsentry, ok := entry.(*FSEntry); ok {
		return fsentry
	}
	return nil
}

// ClearEntryCache ...
func (cache *FileSystemCache) ClearEntryCache() {
	cache.EntryCache.Flush()
}

// AddDirCache ...
func (cache *FileSystemCache) AddDirCache(path string, entries []string) {
	if shouldHaveInfiniteCacheTTL(path) {
		cache.DirCache.Set(path, entries, -1)
	}

	// default
	cache.DirCache.Set(path, entries, 0)
}

// RemoveDirCache ...
func (cache *FileSystemCache) RemoveDirCache(path string) {
	cache.DirCache.Delete(path)
}

// GetDirCache ...
func (cache *FileSystemCache) GetDirCache(path string) []string {
	data, exist := cache.DirCache.Get(path)
	if exist {
		if entries, ok := data.([]string); ok {
			return entries
		}
	}
	return nil
}

// ClearDirCache ...
func (cache *FileSystemCache) ClearDirCache() {
	cache.DirCache.Flush()
}
