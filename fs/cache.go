package fs

import (
	"slices"
	"strings"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// MetadataCacheTimeoutSetting defines cache timeout for path
type MetadataCacheTimeoutSetting struct {
	Path    string         `yaml:"path" json:"path"`
	Timeout types.Duration `yaml:"timeout" json:"timeout"`
	Inherit bool           `yaml:"inherit,omitempty" json:"inherit,omitempty"`
}

// CacheConfig defines cache config
type CacheConfig struct {
	MetadataTimeoutSettings []MetadataCacheTimeoutSetting `yaml:"metadata_timeout_settings,omitempty" json:"metadata_timeout_settings,omitempty"`
	// for mysql iCAT backend, this should be true.
	// for postgresql iCAT backend, this can be false.
	StartNewTransaction bool `yaml:"start_new_transaction,omitempty" json:"start_new_transaction,omitempty"`

	// Backend configuration for caching (memory, ristretto, redis, none)
	Backend *CacheBackendConfig `yaml:"backend,omitempty" json:"backend,omitempty"`

	Logger   *log.Logger // can be nil
	LogEntry *log.Entry  // can be nil
}

// NewDefaultCacheConfig creates a new default CacheConfig
func NewDefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MetadataTimeoutSettings: []MetadataCacheTimeoutSetting{},
		StartNewTransaction:     true,
		Backend:                 NewDefaultCacheBackendConfig(),
	}
}

// Cache namespace names
const (
	cacheNamespaceEntry         = "entries"
	cacheNamespaceNegativeEntry = "negative_entries"
	cacheNamespaceDir           = "dirs"
	cacheNamespaceMetadata      = "metadata"
	cacheNamespaceUser          = "users"
	cacheNamespaceUserList      = "user_lists"
	cacheNamespaceGroupMember   = "group_members"
	cacheNamespaceUserGroup     = "user_groups"
	cacheNamespaceACL           = "acl"
)

// FileSystemCache manages filesystem caches
type FileSystemCache struct {
	id     string
	config *CacheConfig
	logger *log.Entry

	cacheTimeoutPathMap map[string]MetadataCacheTimeoutSetting

	// Account information for cache isolation
	accountID string // hash of (host, account, zone)

	// Cache backend system
	cacheBackend CacheBackend
}

// NewFileSystemCache creates a new FileSystemCache
// accountID: unique account identifier (typically hash of host:port|account|zone)
func NewFileSystemCache(config *CacheConfig, accountID string) *FileSystemCache {
	if config == nil {
		cacheConfig := NewDefaultCacheConfig()
		config = &cacheConfig
	}

	cacheID := xid.New().String()

	var myLogger *log.Entry

	// set logger
	if config != nil && config.LogEntry != nil {
		logFields := log.Fields{
			"cache_id":   cacheID,
			"account_id": accountID,
		}

		myLogger = config.LogEntry.WithFields(logFields)
	} else {
		// create new logger object
		var logger *log.Logger
		if config != nil && config.Logger != nil {
			logger = config.Logger
		} else {
			logger = log.StandardLogger()
		}

		logFields := log.Fields{
			"cache_id":   cacheID,
			"account_id": accountID,
		}

		myLogger = logger.WithFields(logFields)
	}

	// build a map for quick search
	cacheTimeoutSettingMap := map[string]MetadataCacheTimeoutSetting{}
	for _, timeoutSetting := range config.MetadataTimeoutSettings {
		cacheTimeoutSettingMap[timeoutSetting.Path] = timeoutSetting
	}

	// Initialize cache backend
	var cacheBackend CacheBackend
	backendConfig := config.Backend
	if backendConfig == nil {
		backendConfig = NewDefaultCacheBackendConfig()
	}

	factory := NewCacheBackendFactory(backendConfig)
	backend, err := factory.CreateBackend()
	if err != nil {
		myLogger.WithError(err).Warnf("failed to create cache backend %q, falling back to memory backend", backendConfig.Type)

		// Fall back to default memory backend if initialization fails
		defaultFactory := NewCacheBackendFactory(NewDefaultCacheBackendConfig())
		fallbackBackend, _ := defaultFactory.CreateBackend()
		cacheBackend = fallbackBackend
	} else {
		cacheBackend = backend
	}

	return &FileSystemCache{
		config:              config,
		logger:              myLogger,
		cacheTimeoutPathMap: cacheTimeoutSettingMap,
		accountID:           accountID,
		cacheBackend:        cacheBackend,
	}
}

// getNamespace returns a namespace for the given logical namespace name
func (cache *FileSystemCache) getNamespace(logicalNamespace string) CacheNamespace {
	return cache.cacheBackend.GetNamespace(cache.accountID + ":" + logicalNamespace)
}

// deleteNamespace deletes a namespace
func (cache *FileSystemCache) deleteNamespace(logicalNamespace string) error {
	return cache.cacheBackend.DeleteNamespace(cache.accountID + ":" + logicalNamespace)
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
	parentPaths := util.GetIRODSParentDirs(path)
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
	ns := cache.getNamespace(cacheNamespaceEntry)
	_ = ns.Set(entry.Path, entry.clone(), ttl)
}

// RemoveEntryCache removes an entry cache
func (cache *FileSystemCache) RemoveEntryCache(path string) {
	ns := cache.getNamespace(cacheNamespaceEntry)
	_ = ns.Delete(path)
}

// RemoveDirEntryCache removes an entry cache for dir
func (cache *FileSystemCache) RemoveDirEntryCache(path string, recurse bool) {
	ns := cache.getNamespace(cacheNamespaceEntry)
	_ = ns.Delete(path)

	if recurse {
		prefix := strings.TrimSuffix(path, "/") + "/"
		_ = ns.DeletePrefix(prefix)
	}
}

// RemoveParentDirCache removes an entry cache for the parent path of the given path
func (cache *FileSystemCache) RemoveParentDirEntryCache(path string, recurse bool) {
	parentPath := util.GetIRODSPathDirname(path)
	cache.RemoveDirEntryCache(parentPath, recurse)
}

// GetEntryCache retrieves an entry cache
func (cache *FileSystemCache) GetEntryCache(path string) *Entry {
	ns := cache.getNamespace(cacheNamespaceEntry)
	if entry, exist, _ := ns.Get(path); exist {
		if fsentry, ok := entry.(*Entry); ok {
			return fsentry.clone()
		}
	}
	return nil
}

// ClearEntryCache clears all entry caches
func (cache *FileSystemCache) ClearEntryCache() {
	_ = cache.deleteNamespace(cacheNamespaceEntry)
}

// AddNegativeEntryCache adds a negative entry cache
func (cache *FileSystemCache) AddNegativeEntryCache(path string) {
	ttl := cache.getCacheTTLForPath(path)
	ns := cache.getNamespace(cacheNamespaceNegativeEntry)
	_ = ns.Set(path, true, ttl)
}

// RemoveNegativeEntryCache removes a negative entry cache
func (cache *FileSystemCache) RemoveNegativeEntryCache(path string) {
	ns := cache.getNamespace(cacheNamespaceNegativeEntry)
	_ = ns.Delete(path)
}

// RemoveAllNegativeEntryCacheForPath removes all negative entry caches
func (cache *FileSystemCache) RemoveAllNegativeEntryCacheForPath(path string) {
	ns := cache.getNamespace(cacheNamespaceNegativeEntry)
	_ = ns.Delete(path)
	prefix := strings.TrimSuffix(path, "/") + "/"
	_ = ns.DeletePrefix(prefix)
}

// HasNegativeEntryCache checks the existence of a negative entry cache
func (cache *FileSystemCache) HasNegativeEntryCache(path string) bool {
	ns := cache.getNamespace(cacheNamespaceNegativeEntry)
	if exist, existOk, _ := ns.Get(path); existOk {
		if bexist, ok := exist.(bool); ok {
			return bexist
		}
	}
	return false
}

// ClearNegativeEntryCache clears all negative entry caches
func (cache *FileSystemCache) ClearNegativeEntryCache() {
	_ = cache.deleteNamespace(cacheNamespaceNegativeEntry)
}

// AddDirCache adds a dir cache
func (cache *FileSystemCache) AddDirCache(path string, entries []string) {
	ttl := cache.getCacheTTLForPath(path)
	ns := cache.getNamespace(cacheNamespaceDir)
	_ = ns.Set(path, slices.Clone(entries), ttl)
}

// RemoveDirCache removes a dir cache
func (cache *FileSystemCache) RemoveDirCache(path string) {
	ns := cache.getNamespace(cacheNamespaceDir)
	_ = ns.Delete(path)
}

// GetDirCache retrives a dir cache
func (cache *FileSystemCache) GetDirCache(path string) []string {
	ns := cache.getNamespace(cacheNamespaceDir)
	if data, exist, _ := ns.Get(path); exist {
		if entries, ok := data.([]string); ok {
			return slices.Clone(entries)
		}
	}
	return nil
}

// ClearDirCache clears all dir caches
func (cache *FileSystemCache) ClearDirCache() {
	_ = cache.deleteNamespace(cacheNamespaceDir)
}

// AddMetadataCache adds a metadata cache
func (cache *FileSystemCache) AddMetadataCache(path string, metas []*types.IRODSMeta) {
	ttl := cache.getCacheTTLForPath(path)
	ns := cache.getNamespace(cacheNamespaceMetadata)
	_ = ns.Set(path, metas, ttl)
}

// RemoveMetadataCache removes a metadata cache
func (cache *FileSystemCache) RemoveMetadataCache(path string) {
	ns := cache.getNamespace(cacheNamespaceMetadata)
	_ = ns.Delete(path)
}

// GetMetadataCache retrieves a metadata cache
func (cache *FileSystemCache) GetMetadataCache(path string) []*types.IRODSMeta {
	ns := cache.getNamespace(cacheNamespaceMetadata)
	if data, exist, _ := ns.Get(path); exist {
		if metas, ok := data.([]*types.IRODSMeta); ok {
			return metas
		}
	}
	return nil
}

// ClearMetadataCache clears all metadata caches
func (cache *FileSystemCache) ClearMetadataCache() {
	_ = cache.deleteNamespace(cacheNamespaceMetadata)
}

// AddUserCache adds a user cache (cache of a user)
func (cache *FileSystemCache) AddUserCache(user *types.IRODSUser) {
	ns := cache.getNamespace(cacheNamespaceUser + ":" + user.Zone)
	_ = ns.Set(user.Name, user, 0)
}

// AddUserCacheMulti adds multiple user caches (cache of a user)
func (cache *FileSystemCache) AddUserCacheMulti(users []*types.IRODSUser) {
	for _, user := range users {
		cache.AddUserCache(user)
	}
}

// RemoveUserCache removes a user cache (cache of a user)
func (cache *FileSystemCache) RemoveUserCache(username string, zoneName string) {
	ns := cache.getNamespace(cacheNamespaceUser + ":" + zoneName)
	_ = ns.Delete(username)
}

// GetUserCache retrives a user cache (cache of a user)
func (cache *FileSystemCache) GetUserCache(username string, zoneName string) *types.IRODSUser {
	ns := cache.getNamespace(cacheNamespaceUser + ":" + zoneName)
	if user, exist, _ := ns.Get(username); exist {
		if u, ok := user.(*types.IRODSUser); ok {
			return u
		}
	}
	return nil
}

// ClearUserCacheForZone clears user caches for a zone
func (cache *FileSystemCache) ClearUserCacheForZone(zoneName string) {
	_ = cache.deleteNamespace(cacheNamespaceUser + ":" + zoneName)
}

// ClearAllUserCache clears all user caches
func (cache *FileSystemCache) ClearAllUserCache() {
	_ = cache.cacheBackend.Clear()
}

// AddUserListCache adds a user list cache (cache of a list of all user names)
func (cache *FileSystemCache) AddUserListCache(zoneName string, userType types.IRODSUserType, usernames []string) {
	ns := cache.getNamespace(cacheNamespaceUserList + ":" + zoneName)
	_ = ns.Set(string(userType), usernames, 0)
}

// RemoveUserListCache removes a user list cache (cache of a list of all users)
func (cache *FileSystemCache) RemoveUserListCache(zoneName string, userType types.IRODSUserType) {
	ns := cache.getNamespace(cacheNamespaceUserList + ":" + zoneName)
	_ = ns.Delete(string(userType))
}

// GetUserListCache retrives a user list cache (cache of a list of all users)
func (cache *FileSystemCache) GetUserListCache(zoneName string, userType types.IRODSUserType) []string {
	ns := cache.getNamespace(cacheNamespaceUserList + ":" + zoneName)
	if userlist, exist, _ := ns.Get(string(userType)); exist {
		if u, ok := userlist.([]string); ok {
			return u
		}
	}
	return nil
}

// ClearUserListCacheForZone clears all user list caches for a zone
func (cache *FileSystemCache) ClearUserListCacheForZone(zoneName string) {
	_ = cache.deleteNamespace(cacheNamespaceUserList + ":" + zoneName)
}

// ClearAllUserListCache clears all user caches
func (cache *FileSystemCache) ClearAllUserListCache() {
	_ = cache.cacheBackend.Clear()
}

// AddGroupMemberCache adds group member (users in a group) cache
func (cache *FileSystemCache) AddGroupMemberCache(groupName string, zoneName string, usernames []string) {
	ns := cache.getNamespace(cacheNamespaceGroupMember + ":" + zoneName)
	_ = ns.Set(groupName, usernames, 0)
}

// RemoveGroupMemberCache removes group users (users in a group) cache
func (cache *FileSystemCache) RemoveGroupMemberCache(groupName string, zoneName string) {
	ns := cache.getNamespace(cacheNamespaceGroupMember + ":" + zoneName)
	_ = ns.Delete(groupName)
}

// GetGroupMemberCache retrives group members (users in a group) cache
func (cache *FileSystemCache) GetGroupMemberCache(groupName string, zoneName string) []string {
	ns := cache.getNamespace(cacheNamespaceGroupMember + ":" + zoneName)
	if groupMembers, exist, _ := ns.Get(groupName); exist {
		if usernames, ok := groupMembers.([]string); ok {
			return usernames
		}
	}
	return nil
}

// ClearGroupMembersCacheForZone clears all group members (users in a group) caches for a zone
func (cache *FileSystemCache) ClearGroupMembersCacheForZone(zoneName string) {
	_ = cache.deleteNamespace(cacheNamespaceGroupMember + ":" + zoneName)
}

// ClearAllGroupMembersCache clears all group members (users in a group) caches
func (cache *FileSystemCache) ClearAllGroupMembersCache() {
	_ = cache.cacheBackend.Clear()
}

// AddUserGroupCache adds a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) AddUserGroupCache(zoneName string, username string, groupNames []string) {
	ns := cache.getNamespace(cacheNamespaceUserGroup + ":" + zoneName)
	_ = ns.Set(username, groupNames, 0)
}

// RemoveUserGroupCache removes a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) RemoveUserGroupCache(zoneName string, username string) {
	ns := cache.getNamespace(cacheNamespaceUserGroup + ":" + zoneName)
	_ = ns.Delete(username)
}

// GetUserGroupCache retrives a user's groups (groups that a user belongs to) cache
func (cache *FileSystemCache) GetUserGroupCache(zoneName string, username string) []string {
	ns := cache.getNamespace(cacheNamespaceUserGroup + ":" + zoneName)
	if groupNames, exist, _ := ns.Get(username); exist {
		if groups, ok := groupNames.([]string); ok {
			return groups
		}
	}
	return nil
}

// ClearUserGroupCache clears all user's groups caches for a zone
func (cache *FileSystemCache) ClearUserGroupCacheForZone(zoneName string) {
	_ = cache.deleteNamespace(cacheNamespaceUserGroup + ":" + zoneName)
}

// AddAclCache adds a ACLs cache
func (cache *FileSystemCache) AddAclCache(path string, accesses []*types.IRODSAccess) {
	ttl := cache.getCacheTTLForPath(path)
	ns := cache.getNamespace(cacheNamespaceACL)
	_ = ns.Set(path, accesses, ttl)
}

// AddAclCacheMulti adds multiple ACLs caches
func (cache *FileSystemCache) AddAclCacheMulti(accesses []*types.IRODSAccess) {
	m := map[string][]*types.IRODSAccess{}

	for _, access := range accesses {
		if existingAccesses, ok := m[access.Path]; ok {
			existingAccesses = append(existingAccesses, access)
			m[access.Path] = existingAccesses
		} else {
			m[access.Path] = []*types.IRODSAccess{access}
		}
	}

	ns := cache.getNamespace(cacheNamespaceACL)
	for path, access := range m {
		ttl := cache.getCacheTTLForPath(path)
		_ = ns.Set(path, access, ttl)
	}
}

// RemoveAclCache removes a ACLs cache
func (cache *FileSystemCache) RemoveAclCache(path string) {
	ns := cache.getNamespace(cacheNamespaceACL)
	_ = ns.Delete(path)
}

// GetAclCache retrives a ACLs cache
func (cache *FileSystemCache) GetAclCache(path string) []*types.IRODSAccess {
	ns := cache.getNamespace(cacheNamespaceACL)
	if data, exist, _ := ns.Get(path); exist {
		if entries, ok := data.([]*types.IRODSAccess); ok {
			return entries
		}
	}
	return nil
}

// ClearAclCache clears all ACLs caches
func (cache *FileSystemCache) ClearAclCache() {
	_ = cache.deleteNamespace(cacheNamespaceACL)
}

// Close closes the cache backend and releases resources
func (cache *FileSystemCache) Close() {
	if cache.cacheBackend != nil {
		cache.cacheBackend.Close()
	}
}
