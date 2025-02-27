package fs

import (
	"fmt"
	"strings"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	gocache "github.com/patrickmn/go-cache"
)

// MetadataCacheTimeoutSetting defines cache timeout for path
type MetadataCacheTimeoutSetting struct {
	Path    string         `yaml:"path" json:"path"`
	Timeout types.Duration `yaml:"timeout" json:"timeout"`
	Inherit bool           `yaml:"inherit,omitempty" json:"inherit,omitempty"`
}

// CacheConfig defines cache config
type CacheConfig struct {
	Timeout                 types.Duration                `yaml:"timeout,omitempty" json:"timeout,omitempty"`           // cache timeout
	CleanupTime             types.Duration                `yaml:"cleanup_time,omitempty" json:"cleanup_time,omitempty"` // cache cleanup time
	MetadataTimeoutSettings []MetadataCacheTimeoutSetting `yaml:"metadata_timeout_settings,omitempty" json:"metadata_timeout_settings,omitempty"`
	// determine if we will invalidate parent dir's entry cache
	// at subdir/file creation/deletion
	// turn to false to allow short cache inconsistency
	InvalidateParentEntryCacheImmediately bool `yaml:"invalidate_parent_entry_cache_immediately,omitempty" json:"invalidate_parent_entry_cache_immediately,omitempty"`
	// for mysql iCAT backend, this should be true.
	// for postgresql iCAT backend, this can be false.
	StartNewTransaction bool `yaml:"start_new_transaction,omitempty" json:"start_new_transaction,omitempty"`
}

// NewDefaultCacheConfig creates a new default CacheConfig
func NewDefaultCacheConfig() CacheConfig {
	return CacheConfig{
		Timeout:                               types.Duration(FileSystemTimeoutDefault),
		CleanupTime:                           types.Duration(FileSystemTimeoutDefault),
		MetadataTimeoutSettings:               []MetadataCacheTimeoutSetting{},
		InvalidateParentEntryCacheImmediately: true,
		StartNewTransaction:                   true,
	}
}

// FileSystemCache manages filesystem caches
type FileSystemCache struct {
	config *CacheConfig

	cacheTimeoutPathMap map[string]MetadataCacheTimeoutSetting

	entryCache         *gocache.Cache
	negativeEntryCache *gocache.Cache
	dirCache           *gocache.Cache
	metadataCache      *gocache.Cache
	userCache          map[string]*gocache.Cache // zone is key
	userListCache      map[string]*gocache.Cache // zone is key
	groupMemberCache   map[string]*gocache.Cache // zone is key
	userGroupCache     map[string]*gocache.Cache // zone is key
	aclCache           *gocache.Cache
}

// NewFileSystemCache creates a new FileSystemCache
func NewFileSystemCache(config *CacheConfig) *FileSystemCache {
	timeout := time.Duration(config.Timeout)
	cleanupTime := time.Duration(config.CleanupTime)

	entryCache := gocache.New(timeout, cleanupTime)
	negativeEntryCache := gocache.New(timeout, cleanupTime)
	dirCache := gocache.New(timeout, cleanupTime)
	metadataCache := gocache.New(timeout, cleanupTime)
	userCache := map[string]*gocache.Cache{}
	userListCache := map[string]*gocache.Cache{}
	groupUserCache := map[string]*gocache.Cache{}
	userGroupCache := map[string]*gocache.Cache{}
	aclCache := gocache.New(timeout, cleanupTime)

	// build a map for quick search
	cacheTimeoutSettingMap := map[string]MetadataCacheTimeoutSetting{}
	for _, timeoutSetting := range config.MetadataTimeoutSettings {
		cacheTimeoutSettingMap[timeoutSetting.Path] = timeoutSetting
	}

	return &FileSystemCache{
		config: config,

		cacheTimeoutPathMap: cacheTimeoutSettingMap,

		entryCache:         entryCache,
		negativeEntryCache: negativeEntryCache,
		dirCache:           dirCache,
		metadataCache:      metadataCache,
		groupMemberCache:   groupUserCache,
		userGroupCache:     userGroupCache,
		userCache:          userCache,
		userListCache:      userListCache,
		aclCache:           aclCache,
	}
}

func (cache *FileSystemCache) getCacheTTLForPath(path string) time.Duration {
	if len(cache.cacheTimeoutPathMap) == 0 {
		// no data
		return 0
	}

	// check map first
	if timeoutSetting, ok := cache.cacheTimeoutPathMap[path]; ok {
		// exact match
		return time.Duration(timeoutSetting.Timeout)
	}

	// check inherit
	parentPaths := util.GetParentIRODSDirs(path)
	for i := len(parentPaths) - 1; i >= 0; i-- {
		parentPath := parentPaths[i]

		if timeoutSetting, ok := cache.cacheTimeoutPathMap[parentPath]; ok {
			// parent match
			if timeoutSetting.Inherit {
				// inherit
				return time.Duration(timeoutSetting.Timeout)
			}
		}
	}

	// use default
	return 0
}

// AddEntryCache adds an entry cache
func (cache *FileSystemCache) AddEntryCache(entry *Entry) {
	ttl := cache.getCacheTTLForPath(entry.Path)
	cache.entryCache.Set(entry.Path, entry, ttl)
}

// RemoveEntryCache removes an entry cache
func (cache *FileSystemCache) RemoveEntryCache(path string) {
	cache.entryCache.Delete(path)
}

// RemoveDirEntryCache removes an entry cache for dir
func (cache *FileSystemCache) RemoveDirEntryCache(path string, recursive bool) {
	cache.entryCache.Delete(path)

	if recursive {
		prefix := strings.TrimSuffix(path, "/") + "/"

		// recursive
		items := cache.entryCache.Items()
		for k := range items {
			if strings.HasPrefix(k, prefix) {
				// entries under k dir
				cache.entryCache.Delete(k)
			}
		}
	}
}

// RemoveParentDirCache removes an entry cache for the parent path of the given path
func (cache *FileSystemCache) RemoveParentDirEntryCache(path string, recursive bool) {
	if cache.config.InvalidateParentEntryCacheImmediately {
		parentPath := util.GetIRODSPathDirname(path)
		cache.RemoveDirEntryCache(parentPath, recursive)
	}
}

// GetEntryCache retrieves an entry cache
func (cache *FileSystemCache) GetEntryCache(path string) *Entry {
	if entry, exist := cache.entryCache.Get(path); exist {
		if fsentry, ok := entry.(*Entry); ok {
			return fsentry
		}
	}
	return nil
}

// ClearEntryCache clears all entry caches
func (cache *FileSystemCache) ClearEntryCache() {
	cache.entryCache.Flush()
}

// AddNegativeEntryCache adds a negative entry cache
func (cache *FileSystemCache) AddNegativeEntryCache(path string) {
	ttl := cache.getCacheTTLForPath(path)
	cache.negativeEntryCache.Set(path, true, ttl)
}

// RemoveNegativeEntryCache removes a negative entry cache
func (cache *FileSystemCache) RemoveNegativeEntryCache(path string) {
	cache.negativeEntryCache.Delete(path)
}

// RemoveAllNegativeEntryCacheForPath removes all negative entry caches
func (cache *FileSystemCache) RemoveAllNegativeEntryCacheForPath(path string) {
	prefix := fmt.Sprintf("%s/", path)
	deleteKey := []string{}
	for k := range cache.negativeEntryCache.Items() {
		if k == path || strings.HasPrefix(k, prefix) {
			deleteKey = append(deleteKey, k)
		}
	}

	for _, k := range deleteKey {
		cache.negativeEntryCache.Delete(k)
	}
}

// HasNegativeEntryCache checks the existence of a negative entry cache
func (cache *FileSystemCache) HasNegativeEntryCache(path string) bool {
	if exist, existOk := cache.negativeEntryCache.Get(path); existOk {
		if bexist, ok := exist.(bool); ok {
			return bexist
		}
	}
	return false
}

// ClearNegativeEntryCache clears all negative entry caches
func (cache *FileSystemCache) ClearNegativeEntryCache() {
	cache.negativeEntryCache.Flush()
}

// AddDirCache adds a dir cache
func (cache *FileSystemCache) AddDirCache(path string, entries []string) {
	ttl := cache.getCacheTTLForPath(path)
	cache.dirCache.Set(path, entries, ttl)
}

// RemoveDirCache removes a dir cache
func (cache *FileSystemCache) RemoveDirCache(path string) {
	cache.dirCache.Delete(path)
}

// GetDirCache retrives a dir cache
func (cache *FileSystemCache) GetDirCache(path string) []string {
	data, exist := cache.dirCache.Get(path)
	if exist {
		if entries, ok := data.([]string); ok {
			return entries
		}
	}
	return nil
}

// ClearDirCache clears all dir caches
func (cache *FileSystemCache) ClearDirCache() {
	cache.dirCache.Flush()
}

// AddMetadataCache adds a metadata cache
func (cache *FileSystemCache) AddMetadataCache(path string, metas []*types.IRODSMeta) {
	ttl := cache.getCacheTTLForPath(path)
	cache.metadataCache.Set(path, metas, ttl)
}

// RemoveMetadataCache removes a metadata cache
func (cache *FileSystemCache) RemoveMetadataCache(path string) {
	cache.metadataCache.Delete(path)
}

// GetMetadataCache retrieves a metadata cache
func (cache *FileSystemCache) GetMetadataCache(path string) []*types.IRODSMeta {
	data, exist := cache.metadataCache.Get(path)
	if exist {
		if metas, ok := data.([]*types.IRODSMeta); ok {
			return metas
		}
	}
	return nil
}

// ClearMetadataCache clears all metadata caches
func (cache *FileSystemCache) ClearMetadataCache() {
	cache.metadataCache.Flush()
}

// AddUserCache adds a user cache (cache of a user)
func (cache *FileSystemCache) AddUserCache(user *types.IRODSUser) {
	userCacheForZone := cache.userCache[user.Zone]
	if userCacheForZone == nil {
		timeout := time.Duration(cache.config.Timeout)
		cleanupTime := time.Duration(cache.config.CleanupTime)

		// create cache if not exist
		userCacheForZone = gocache.New(timeout, cleanupTime)
		cache.userCache[user.Zone] = userCacheForZone
	}

	userCacheForZone.Set(user.Name, user, 0)
}

// AddUserCacheMulti adds multiple user caches (cache of a user)
func (cache *FileSystemCache) AddUserCacheMulti(users []*types.IRODSUser) {
	for _, user := range users {
		cache.AddUserCache(user)
	}
}

// RemoveUserCache removes a user cache (cache of a user)
func (cache *FileSystemCache) RemoveUserCache(username string, zoneName string) {
	userCacheForZone := cache.userCache[zoneName]
	if userCacheForZone == nil {
		return
	}

	userCacheForZone.Delete(username)
}

// GetUserCache retrives a user cache (cache of a user)
func (cache *FileSystemCache) GetUserCache(username string, zoneName string) *types.IRODSUser {
	userCacheForZone := cache.userCache[zoneName]
	if userCacheForZone == nil {
		return nil
	}

	user, exist := userCacheForZone.Get(username)
	if exist {
		if user, ok := user.(*types.IRODSUser); ok {
			return user
		}
	}

	return nil
}

// ClearUserCacheForZone clears user caches for a zone
func (cache *FileSystemCache) ClearUserCacheForZone(zoneName string) {
	userCacheForZone := cache.userCache[zoneName]
	if userCacheForZone == nil {
		return
	}

	userCacheForZone.Flush()
}

// ClearAllUserCache clears all user caches
func (cache *FileSystemCache) ClearAllUserCache() {
	for _, userCacheForZone := range cache.userCache {
		userCacheForZone.Flush()
	}
}

// AddUserListCache adds a user list cache (cache of a list of all user names)
func (cache *FileSystemCache) AddUserListCache(zoneName string, userType types.IRODSUserType, usernames []string) {
	userListCacheForZone := cache.userListCache[zoneName]
	if userListCacheForZone == nil {
		timeout := time.Duration(cache.config.Timeout)
		cleanupTime := time.Duration(cache.config.CleanupTime)

		// create cache if not exist
		userListCacheForZone = gocache.New(timeout, cleanupTime)
		cache.userListCache[zoneName] = userListCacheForZone
	}

	userListCacheForZone.Set(string(userType), usernames, 0)
}

// RemoveUserListCache removes a user list cache (cache of a list of all users)
func (cache *FileSystemCache) RemoveUserListCache(zoneName string, userType types.IRODSUserType) {
	userListCacheForZone := cache.userListCache[zoneName]
	if userListCacheForZone == nil {
		return
	}

	userListCacheForZone.Delete(string(userType))
}

// GetUserListCache retrives a user list cache (cache of a list of all users)
func (cache *FileSystemCache) GetUserListCache(zoneName string, userType types.IRODSUserType) []string {
	userListCacheForZone := cache.userListCache[zoneName]
	if userListCacheForZone == nil {
		return nil
	}

	userlist, exist := userListCacheForZone.Get(string(userType))
	if exist {
		if user, ok := userlist.([]string); ok {
			return user
		}
	}

	return nil
}

// ClearUserListCacheForZone clears all user list caches for a zone
func (cache *FileSystemCache) ClearUserListCacheForZone(zoneName string) {
	userListCacheForZone := cache.userListCache[zoneName]
	if userListCacheForZone == nil {
		return
	}

	userListCacheForZone.Flush()
}

// ClearAllUserListCache clears all user caches
func (cache *FileSystemCache) ClearAllUserListCache() {
	for _, userListCacheForZone := range cache.userListCache {
		userListCacheForZone.Flush()
	}
}

// AddGroupMemberCache adds group member (users in a group) cache
func (cache *FileSystemCache) AddGroupMemberCache(groupName string, zoneName string, usernames []string) {
	groupMemberCacheForZone := cache.groupMemberCache[zoneName]
	if groupMemberCacheForZone == nil {
		timeout := time.Duration(cache.config.Timeout)
		cleanupTime := time.Duration(cache.config.CleanupTime)

		// create cache if not exist
		groupMemberCacheForZone = gocache.New(timeout, cleanupTime)
		cache.groupMemberCache[zoneName] = groupMemberCacheForZone
	}

	groupMemberCacheForZone.Set(groupName, usernames, 0)
}

// RemoveGroupMemberCache removes group users (users in a group) cache
func (cache *FileSystemCache) RemoveGroupMemberCache(groupName string, zoneName string) {
	groupMemberCacheForZone := cache.groupMemberCache[zoneName]
	if groupMemberCacheForZone == nil {
		return
	}

	groupMemberCacheForZone.Delete(groupName)
}

// GetGroupMemberCache retrives group members (users in a group) cache
func (cache *FileSystemCache) GetGroupMemberCache(groupName string, zoneName string) []string {
	groupMemberCacheForZone := cache.groupMemberCache[zoneName]
	if groupMemberCacheForZone == nil {
		return nil
	}

	groupMembers, exist := groupMemberCacheForZone.Get(groupName)
	if exist {
		if usernames, ok := groupMembers.([]string); ok {
			return usernames
		}
	}

	return nil
}

// ClearGroupMembersCacheForZone clears all group members (users in a group) caches for a zone
func (cache *FileSystemCache) ClearGroupMembersCacheForZone(zoneName string) {
	groupMemberCacheForZone := cache.groupMemberCache[zoneName]
	if groupMemberCacheForZone == nil {
		return
	}

	groupMemberCacheForZone.Flush()
}

// ClearAllGroupMembersCache clears all group members (users in a group) caches
func (cache *FileSystemCache) ClearAllGroupMembersCache() {
	for _, groupMemberCacheForZone := range cache.groupMemberCache {
		groupMemberCacheForZone.Flush()
	}
}

// AddUserGroupCache adds a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) AddUserGroupCache(zoneName string, username string, groupNames []string) {
	userGroupCacheForZone := cache.userGroupCache[zoneName]
	if userGroupCacheForZone == nil {
		timeout := time.Duration(cache.config.Timeout)
		cleanupTime := time.Duration(cache.config.CleanupTime)

		// create cache if not exist
		userGroupCacheForZone = gocache.New(timeout, cleanupTime)
		cache.userGroupCache[zoneName] = userGroupCacheForZone
	}

	userGroupCacheForZone.Set(username, groupNames, 0)
}

// RemoveUserGroupCache removes a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) RemoveUserGroupCache(zoneName string, username string) {
	userGroupCacheForZone := cache.userGroupCache[zoneName]
	if userGroupCacheForZone == nil {
		return
	}

	userGroupCacheForZone.Delete(username)
}

// GetUserGroupCache retrives a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) GetUserGroupCache(zoneName string, username string) []string {
	userGroupCacheForZone := cache.userGroupCache[zoneName]
	if userGroupCacheForZone == nil {
		return nil
	}

	groupNames, exist := userGroupCacheForZone.Get(username)
	if exist {
		if groups, ok := groupNames.([]string); ok {
			return groups
		}
	}

	return nil
}

// ClearUserGroupCache clears all user's groups caches for a zone
func (cache *FileSystemCache) ClearUserGroupCacheForZone(zoneName string) {
	userGroupCacheForZone := cache.userGroupCache[zoneName]
	if userGroupCacheForZone == nil {
		return
	}

	userGroupCacheForZone.Flush()
}

// AddAclCache adds a ACLs cache
func (cache *FileSystemCache) AddAclCache(path string, accesses []*types.IRODSAccess) {
	ttl := cache.getCacheTTLForPath(path)
	cache.aclCache.Set(path, accesses, ttl)
}

// AddAclCacheMulti adds multiple ACLs caches
func (cache *FileSystemCache) AddAclCacheMulti(accesses []*types.IRODSAccess) {
	m := map[string][]*types.IRODSAccess{}

	for _, access := range accesses {
		if existingAccesses, ok := m[access.Path]; ok {
			// has it, add
			existingAccesses = append(existingAccesses, access)
			m[access.Path] = existingAccesses
		} else {
			// create it
			m[access.Path] = []*types.IRODSAccess{access}
		}
	}

	for path, access := range m {
		ttl := cache.getCacheTTLForPath(path)
		cache.aclCache.Set(path, access, ttl)
	}
}

// RemoveAclCache removes a ACLs cache
func (cache *FileSystemCache) RemoveAclCache(path string) {
	cache.aclCache.Delete(path)
}

// GetAclCache retrives a ACLs cache
func (cache *FileSystemCache) GetAclCache(path string) []*types.IRODSAccess {
	data, exist := cache.aclCache.Get(path)
	if exist {
		if entries, ok := data.([]*types.IRODSAccess); ok {
			return entries
		}
	}
	return nil
}

// ClearAclCache clears all ACLs caches
func (cache *FileSystemCache) ClearAclCache() {
	cache.aclCache.Flush()
}
