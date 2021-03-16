package fs

import (
	"fmt"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	gocache "github.com/patrickmn/go-cache"
)

// FileSystemCache ...
type FileSystemCache struct {
	CacheTimeout    time.Duration
	CleanupTimeout  time.Duration
	EntryCache      *gocache.Cache
	DirCache        *gocache.Cache
	MetadataCache   *gocache.Cache
	GroupUsersCache *gocache.Cache
}

// NewFileSystemCache creates a new FileSystemCache
func NewFileSystemCache(cacheTimeout time.Duration, cleanup time.Duration) *FileSystemCache {
	entryCache := gocache.New(cacheTimeout, cleanup)
	dirCache := gocache.New(cacheTimeout, cleanup)
	metadataCache := gocache.New(cacheTimeout, cleanup)
	groupUsersCache := gocache.New(cacheTimeout, cleanup)

	return &FileSystemCache{
		CacheTimeout:    cacheTimeout,
		CleanupTimeout:  cleanup,
		EntryCache:      entryCache,
		DirCache:        dirCache,
		MetadataCache:   metadataCache,
		GroupUsersCache: groupUsersCache,
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

// AddMetadataCache ...
func (cache *FileSystemCache) AddMetadataCache(path string, metas []*types.IRODSMeta) {
	if shouldHaveInfiniteCacheTTL(path) {
		cache.MetadataCache.Set(path, metas, -1)
	}

	// default
	cache.MetadataCache.Set(path, metas, 0)
}

// RemoveMetadataCache ...
func (cache *FileSystemCache) RemoveMetadataCache(path string) {
	cache.MetadataCache.Delete(path)
}

// GetMetadataCache ...
func (cache *FileSystemCache) GetMetadataCache(path string) []*types.IRODSMeta {
	data, exist := cache.MetadataCache.Get(path)
	if exist {
		if metas, ok := data.([]*types.IRODSMeta); ok {
			return metas
		}
	}
	return nil
}

// ClearMetadataCache ...
func (cache *FileSystemCache) ClearMetadataCache() {
	cache.MetadataCache.Flush()
}

// AddGroupUsersCache ...
func (cache *FileSystemCache) AddGroupUsersCache(group string, users []types.IRODSUser) {
	cache.GroupUsersCache.Set(group, users, 0)
}

// RemoveGroupUsersCache ...
func (cache *FileSystemCache) RemoveGroupUsersCache(group string) {
	cache.GroupUsersCache.Delete(group)
}

// GetGroupUsersCache ...
func (cache *FileSystemCache) GetGroupUsersCache(group string) []types.IRODSUser {
	users, exist := cache.GroupUsersCache.Get(group)
	if exist {
		if irodsUsers, ok := users.([]types.IRODSUser); ok {
			return irodsUsers
		}
	}
	return nil
}
