package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	id               string
	account          *types.IRODSAccount
	config           *FileSystemConfig
	session          *session.IRODSSession
	cache            *FileSystemCache
	cachePropagation *FileSystemCachePropagation
	fileHandleMap    *FileHandleMap
}

// NewFileSystem creates a new FileSystem
func NewFileSystem(account *types.IRODSAccount, config *FileSystemConfig) (*FileSystem, error) {
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.ConnectionLifespan, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime, config.CacheTimeoutSettings, config.InvalidateParentEntryCacheImmediately)

	fs := &FileSystem{
		id:            xid.New().String(), // generate a new ID
		account:       account,
		config:        config,
		session:       sess,
		cache:         cache,
		fileHandleMap: NewFileHandleMap(),
	}

	cachePropagation := NewFileSystemCachePropagation(fs)
	fs.cachePropagation = cachePropagation

	return fs, nil
}

// NewFileSystemWithDefault creates a new FileSystem with default configurations
func NewFileSystemWithDefault(account *types.IRODSAccount, applicationName string) (*FileSystem, error) {
	config := NewFileSystemConfigWithDefault(applicationName)
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.ConnectionLifespan, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime, config.CacheTimeoutSettings, config.InvalidateParentEntryCacheImmediately)

	fs := &FileSystem{
		id:            xid.New().String(), // generate a new ID
		account:       account,
		config:        config,
		session:       sess,
		cache:         cache,
		fileHandleMap: NewFileHandleMap(),
	}

	cachePropagation := NewFileSystemCachePropagation(fs)
	fs.cachePropagation = cachePropagation

	return fs, nil
}

// NewFileSystemWithSessionConfig creates a new FileSystem with custom session configurations
func NewFileSystemWithSessionConfig(account *types.IRODSAccount, sessConfig *session.IRODSSessionConfig) (*FileSystem, error) {
	config := NewFileSystemConfigWithDefault(sessConfig.ApplicationName)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime, config.CacheTimeoutSettings, config.InvalidateParentEntryCacheImmediately)

	fs := &FileSystem{
		id:            xid.New().String(), // generate a new ID
		account:       account,
		config:        config,
		session:       sess,
		cache:         cache,
		fileHandleMap: NewFileHandleMap(),
	}

	cachePropagation := NewFileSystemCachePropagation(fs)
	fs.cachePropagation = cachePropagation

	return fs, nil
}

// Release releases all resources
func (fs *FileSystem) Release() {
	handles := fs.fileHandleMap.PopAll()
	for _, handle := range handles {
		handle.closeWithoutFSHandleManagement()
	}

	fs.cachePropagation.Release()

	fs.session.Release()
}

// GetID returns file system instance ID
func (fs *FileSystem) GetID() string {
	return fs.id
}

// Connections counts current established connections
func (fs *FileSystem) Connections() int {
	return fs.session.Connections()
}

// GetTransferMetrics returns transfer metrics
func (fs *FileSystem) GetTransferMetrics() types.TransferMetrics {
	return fs.session.GetTransferMetrics()
}

// ListGroupUsers lists all users in a group
func (fs *FileSystem) ListGroupUsers(group string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetGroupUsersCache(group)
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	users, err := irods_fs.ListGroupUsers(conn, group)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddGroupUsersCache(group, users)

	return users, nil
}

// ListGroups lists all groups
func (fs *FileSystem) ListGroups() ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.cache.GetGroupsCache()
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	groups, err := irods_fs.ListGroups(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddGroupsCache(groups)

	return groups, nil
}

// ListUserGroups lists all groups that a user belongs to
func (fs *FileSystem) ListUserGroups(user string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.cache.GetUserGroupsCache(user)
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	groupNames, err := irods_fs.ListUserGroupNames(conn, user)
	if err != nil {
		return nil, err
	}

	groups := []*types.IRODSUser{}
	for _, groupName := range groupNames {
		group, err := irods_fs.GetGroup(conn, groupName)
		if err != nil {
			return nil, err
		}

		groups = append(groups, group)
	}

	// cache it
	fs.cache.AddUserGroupsCache(user, groups)

	return groups, nil
}

// ListUsers lists all users
func (fs *FileSystem) ListUsers() ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetUsersCache()
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	users, err := irods_fs.ListUsers(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUsersCache(users)

	return users, nil
}

// Stat returns file status
func (fs *FileSystem) Stat(path string) (*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check if a negative cache for the given path exists
	if fs.cache.HasNegativeEntryCache(irodsPath) {
		// has a negative cache - fail fast
		return nil, types.NewFileNotFoundError("could not find a data object or a directory")
	}

	// check if a cached Entry for the given path exists
	cachedEntry := fs.cache.GetEntryCache(irodsPath)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// check if a cached dir Entry for the given path exists
	parentPath := util.GetDirname(irodsPath)
	cachedDirEntryPaths := fs.cache.GetDirCache(parentPath)
	dirEntryExist := false
	if cachedDirEntryPaths != nil {
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			if cachedDirEntryPath == irodsPath {
				dirEntryExist = true
				break
			}
		}

		if !dirEntryExist {
			// dir entry not exist - fail fast
			fs.cache.AddNegativeEntryCache(irodsPath)
			return nil, types.NewFileNotFoundError("could not find a data object or a directory")
		}
	}

	// if cache does not exist,
	// check dir first
	dirStat, err := fs.getCollectionNoCache(irodsPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return dirStat, nil
	}

	// if it's not dir, check file
	fileStat, err := fs.getDataObjectNoCache(irodsPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return fileStat, nil
	}

	// not a collection, not a data object
	fs.cache.AddNegativeEntryCache(irodsPath)
	return nil, types.NewFileNotFoundError("could not find a data object or a directory")
}

// StatDir returns status of a directory
func (fs *FileSystem) StatDir(path string) (*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	return fs.getCollection(irodsPath)
}

// StatFile returns status of a file
func (fs *FileSystem) StatFile(path string) (*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	return fs.getDataObject(irodsPath)
}

// Exists checks file/directory existence
func (fs *FileSystem) Exists(path string) bool {
	entry, err := fs.Stat(path)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// ExistsDir checks directory existence
func (fs *FileSystem) ExistsDir(path string) bool {
	entry, err := fs.StatDir(path)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// ExistsFile checks file existence
func (fs *FileSystem) ExistsFile(path string) bool {
	entry, err := fs.StatFile(path)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// ListACLs returns ACLs
func (fs *FileSystem) ListACLs(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	if stat.Type == DirectoryEntry {
		return fs.ListDirACLs(path)
	} else if stat.Type == FileEntry {
		return fs.ListFileACLs(path)
	}

	return nil, fmt.Errorf("unknown type - %s", stat.Type)
}

// ListACLsWithGroupUsers returns ACLs
func (fs *FileSystem) ListACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}
	if stat.Type == DirectoryEntry {
		accessList, err := fs.ListDirACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else if stat.Type == FileEntry {
		accessList, err := fs.ListFileACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else {
		return nil, fmt.Errorf("unknown type - %s", stat.Type)
	}

	return accesses, nil
}

// ListDirACLs returns ACLs of a directory
func (fs *FileSystem) ListDirACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.cache.GetDirACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	accesses, err := irods_fs.ListCollectionAccess(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddDirACLsCache(irodsPath, accesses)

	return accesses, nil
}

// ListDirACLsWithGroupUsers returns ACLs of a directory
// CAUTION: this can fail if a group contains a lot of users
func (fs *FileSystem) ListDirACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListDirACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all users in the group
			users, err := fs.ListGroupUsers(access.UserName)
			if err != nil {
				return nil, err
			}

			for _, user := range users {
				userAccess := &types.IRODSAccess{
					Path:        access.Path,
					UserName:    user.Name,
					UserZone:    user.Zone,
					UserType:    user.Type,
					AccessLevel: access.AccessLevel,
				}

				// remove duplicates
				newAccessesMap[fmt.Sprintf("%s||%s", user.Name, access.AccessLevel)] = userAccess
			}
		} else {
			newAccessesMap[fmt.Sprintf("%s||%s", access.UserName, access.AccessLevel)] = access
		}
	}

	// convert map to array
	for _, access := range newAccessesMap {
		newAccesses = append(newAccesses, access)
	}

	return newAccesses, nil
}

// ListFileACLs returns ACLs of a file
func (fs *FileSystem) ListFileACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.cache.GetFileACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collectionEntry, err := fs.getCollection(util.GetIRODSPathDirname(irodsPath))
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	accesses, err := irods_fs.ListDataObjectAccess(conn, collection, util.GetIRODSPathFileName(irodsPath))
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddFileACLsCache(irodsPath, accesses)

	return accesses, nil
}

// ListFileACLsWithGroupUsers returns ACLs of a file
func (fs *FileSystem) ListFileACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListFileACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all users in the group
			users, err := fs.ListGroupUsers(access.UserName)
			if err != nil {
				return nil, err
			}

			for _, user := range users {
				userAccess := &types.IRODSAccess{
					Path:        access.Path,
					UserName:    user.Name,
					UserZone:    user.Zone,
					UserType:    user.Type,
					AccessLevel: access.AccessLevel,
				}

				// remove duplicates
				newAccessesMap[fmt.Sprintf("%s||%s", user.Name, access.AccessLevel)] = userAccess
			}
		} else {
			newAccessesMap[fmt.Sprintf("%s||%s", access.UserName, access.AccessLevel)] = access
		}
	}

	// convert map to array
	for _, access := range newAccessesMap {
		newAccesses = append(newAccesses, access)
	}

	return newAccesses, nil
}

// List lists all file system entries under the given path
func (fs *FileSystem) List(path string) ([]*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	collectionEntry, err := fs.getCollection(irodsPath)
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	return fs.listEntries(collection)
}

// SearchByMeta searches all file system entries with given metadata
func (fs *FileSystem) SearchByMeta(metaname string, metavalue string) ([]*Entry, error) {
	return fs.searchEntriesByMeta(metaname, metavalue)
}

// RemoveDir deletes a directory
func (fs *FileSystem) RemoveDir(path string, recurse bool, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.DeleteCollection(conn, irodsPath, recurse, force)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirRemove(irodsPath, recurse)
	fs.cachePropagation.PropagateDirRemove(irodsPath)
	return nil
}

// RemoveFile deletes a file
func (fs *FileSystem) RemoveFile(path string, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.DeleteDataObject(conn, irodsPath, force)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileRemove(irodsPath)
	fs.cachePropagation.PropagateFileRemove(irodsPath)
	return nil
}

// RenameDir renames a dir
func (fs *FileSystem) RenameDir(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	destDirPath := irodsDestPath
	if fs.ExistsDir(irodsDestPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(irodsSrcPath)
		destDirPath = util.MakeIRODSPath(irodsDestPath, srcFileName)
	}

	return fs.RenameDirToDir(irodsSrcPath, destDirPath)
}

// RenameDirToDir renames a dir
func (fs *FileSystem) RenameDirToDir(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	// preprocess
	handles, err := fs.preprocessRenameFileHandleForDir(irodsSrcPath)
	if err != nil {
		return err
	}

	err = irods_fs.MoveCollection(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirRemove(irodsSrcPath, true)
	fs.cachePropagation.PropagateDirRemove(irodsSrcPath)
	fs.invalidateCacheForDirCreate(irodsDestPath)
	fs.cachePropagation.PropagateDirCreate(irodsDestPath)

	// postprocess
	err = fs.postprocessRenameFileHandleForDir(handles, conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}
	return nil
}

// RenameFile renames a file
func (fs *FileSystem) RenameFile(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	destFilePath := irodsDestPath
	if fs.ExistsDir(irodsDestPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(irodsSrcPath)
		destFilePath = util.MakeIRODSPath(irodsDestPath, srcFileName)
	}

	return fs.RenameFileToFile(irodsSrcPath, destFilePath)
}

// RenameFileToFile renames a file
func (fs *FileSystem) RenameFileToFile(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	// preprocess
	handles, err := fs.preprocessRenameFileHandle(irodsSrcPath)
	if err != nil {
		return err
	}

	// rename
	err = irods_fs.MoveDataObject(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileRemove(irodsSrcPath)
	fs.cachePropagation.PropagateFileRemove(irodsSrcPath)
	fs.invalidateCacheForFileCreate(irodsDestPath)
	fs.cachePropagation.PropagateFileCreate(irodsDestPath)

	// postprocess
	err = fs.postprocessRenameFileHandle(handles, conn, irodsDestPath)
	if err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) preprocessRenameFileHandle(srcPath string) ([]*FileHandle, error) {
	handles := fs.fileHandleMap.PopByPath(srcPath)
	handlesLocked := []*FileHandle{}

	errs := []error{}
	for _, handle := range handles {
		// lock handles
		handle.Lock()

		err := handle.preprocessRename()
		if err != nil {
			errs = append(errs, err)
			// unlock handle
			handle.Unlock()
		} else {
			handlesLocked = append(handlesLocked, handle)
		}
	}

	if len(errs) > 0 {
		return handlesLocked, errs[0]
	}
	return handlesLocked, nil
}

func (fs *FileSystem) preprocessRenameFileHandleForDir(srcPath string) ([]*FileHandle, error) {
	paths := fs.fileHandleMap.ListPathsInDir(srcPath)

	errs := []error{}
	handles := []*FileHandle{}
	for _, path := range paths {
		handlesForPath, err := fs.preprocessRenameFileHandle(path)
		if err != nil {
			errs = append(errs, err)
		} else {
			handles = append(handles, handlesForPath...)
		}
	}

	if len(errs) > 0 {
		return handles, errs[0]
	}
	return handles, nil
}

func (fs *FileSystem) postprocessRenameFileHandle(handles []*FileHandle, conn *connection.IRODSConnection, destPath string) error {
	newEntry, err := fs.getDataObjectWithConnection(conn, destPath)
	if err != nil {
		return err
	}

	errs := []error{}
	for _, handle := range handles {
		err := handle.postprocessRename(destPath, newEntry)
		if err != nil {
			errs = append(errs, err)
		}

		handle.Unlock()
		fs.fileHandleMap.Add(handle)
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (fs *FileSystem) postprocessRenameFileHandleForDir(handles []*FileHandle, conn *connection.IRODSConnection, srcPath string, destPath string) error {
	errs := []error{}

	// map (original path => new Entry)
	entryMap := map[string]*Entry{}
	for _, handle := range handles {
		if _, ok := entryMap[handle.entry.Path]; !ok {
			// mapping not exist
			// make full destPath
			relPath, err := util.GetRelativePath(srcPath, handle.entry.Path)
			if err != nil {
				errs = append(errs, err)
			} else {
				destFullPath := util.JoinPath(destPath, relPath)
				newEntry, err := fs.getDataObjectWithConnection(conn, destFullPath)
				if err != nil {
					errs = append(errs, err)
				} else {
					entryMap[handle.entry.Path] = newEntry
				}
			}
		}
	}

	if len(errs) > 0 {
		for _, handle := range handles {
			handle.Unlock()
		}
		return errs[0]
	}

	for _, handle := range handles {
		newEntry := entryMap[handle.entry.Path]
		err := handle.postprocessRename(newEntry.Path, newEntry)
		if err != nil {
			errs = append(errs, err)
		}

		handle.Unlock()
		fs.fileHandleMap.Add(handle)
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// MakeDir creates a directory
func (fs *FileSystem) MakeDir(path string, recurse bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.CreateCollection(conn, irodsPath, recurse)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirCreate(irodsPath)
	fs.cachePropagation.PropagateDirCreate(irodsPath)
	fs.cache.AddDirCache(irodsPath, []string{})
	return nil
}

// CopyFile copies a file
func (fs *FileSystem) CopyFile(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	destFilePath := irodsDestPath
	if fs.ExistsDir(irodsDestPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(irodsSrcPath)
		destFilePath = util.MakeIRODSPath(irodsDestPath, srcFileName)
	}

	return fs.CopyFileToFile(irodsSrcPath, destFilePath)
}

// CopyFileToFile copies a file
func (fs *FileSystem) CopyFileToFile(srcPath string, destPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.CopyDataObject(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsDestPath)
	fs.cachePropagation.PropagateFileCreate(irodsDestPath)
	return nil
}

// TruncateFile truncates a file
func (fs *FileSystem) TruncateFile(path string, size int64) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	if size < 0 {
		size = 0
	}

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.TruncateDataObject(conn, irodsPath, size)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsPath)
	fs.cachePropagation.PropagateFileUpdate(irodsPath)
	return nil
}

// ReplicateFile replicates a file
func (fs *FileSystem) ReplicateFile(path string, resource string, update bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.ReplicateDataObject(conn, irodsPath, resource, update, false)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsPath)
	fs.cachePropagation.PropagateFileUpdate(irodsPath)
	return nil
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	if srcStat.Type == DirectoryEntry {
		return fmt.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			return fmt.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObject(fs.session, irodsSrcPath, resource, localFilePath)
}

// DownloadFileParallel downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	if srcStat.Type == DirectoryEntry {
		return fmt.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			return fmt.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObjectParallel(fs.session, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum)
}

// DownloadFileParallelInBlocksAsync downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallelInBlocksAsync(irodsPath string, resource string, localPath string, blockLength int64, taskNum int) (chan int64, chan error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		errChan <- types.NewFileNotFoundErrorf("could not find a data object")
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if srcStat.Type == DirectoryEntry {
		errChan <- fmt.Errorf("cannot download a collection %s", irodsSrcPath)
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			errChan <- err
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			errChan <- fmt.Errorf("file %s already exists", localDestPath)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	return irods_fs.DownloadDataObjectParallelInBlocksAsync(fs.session, irodsSrcPath, resource, localFilePath, srcStat.Size, blockLength, taskNum)
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool) error {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return types.NewFileNotFoundError("could not find the local file")
		}
		return err
	}

	if stat.IsDir() {
		return types.NewFileNotFoundError("The local file is a directory")
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fmt.Errorf("unknown entry type %s", entry.Type)
		}
	}

	err = irods_fs.UploadDataObject(fs.session, localSrcPath, irodsFilePath, resource, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool) error {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return types.NewFileNotFoundError("could not find the local file")
		}
		return err
	}

	if srcStat.IsDir() {
		return types.NewFileNotFoundError("The local file is a directory")
	}

	destStat, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch destStat.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fmt.Errorf("unknown entry type %s", destStat.Type)
		}
	}

	err = irods_fs.UploadDataObjectParallel(fs.session, localSrcPath, irodsFilePath, resource, taskNum, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallelInBlocksAsync uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallelInBlocksAsync(localPath string, irodsPath string, resource string, blockLength int64, taskNum int, replicate bool) (chan int64, chan error) {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			errChan <- types.NewFileNotFoundError("could not find the local file")
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}

		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if srcStat.IsDir() {
		errChan <- types.NewFileNotFoundError("The local file is a directory")
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	destStat, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			errChan <- err
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	} else {
		switch destStat.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			errChan <- fmt.Errorf("unknown entry type %s", destStat.Type)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	outputChan2, errChan2 := irods_fs.UploadDataObjectParallelInBlockAsync(fs.session, localSrcPath, irodsFilePath, resource, blockLength, taskNum, replicate)

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return outputChan2, errChan2
}

// OpenFile opens an existing file for read/write
func (fs *FileSystem) OpenFile(path string, resource string, mode string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}

	handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, mode)
	if err != nil {
		fs.session.ReturnConnection(conn)
		return nil, err
	}

	var entry *Entry = nil
	openMode := types.FileOpenMode(mode)
	if openMode.IsOpeningExisting() {
		// file may exists
		entryExisting, err := fs.getDataObjectWithConnection(conn, irodsPath)
		if err == nil {
			entry = entryExisting
		}
	}

	if entry == nil {
		// create a new
		entry = &Entry{
			ID:         0,
			Type:       FileEntry,
			Name:       util.GetIRODSPathFileName(irodsPath),
			Path:       irodsPath,
			Owner:      fs.account.ClientUser,
			Size:       0,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			CheckSum:   "",
		}
	}

	// do not return connection here
	fileHandle := &FileHandle{
		id:              xid.New().String(),
		filesystem:      fs,
		connection:      conn,
		irodsfilehandle: handle,
		entry:           entry,
		offset:          offset,
		openmode:        types.FileOpenMode(mode),
	}

	fs.fileHandleMap.Add(fileHandle)
	return fileHandle, nil
}

// CreateFile opens a new file for write
func (fs *FileSystem) CreateFile(path string, resource string, mode string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}

	// create
	handle, err := irods_fs.CreateDataObject(conn, irodsPath, resource, mode, true)
	if err != nil {
		fs.session.ReturnConnection(conn)
		return nil, err
	}

	// close - this is required to let other processes see the file existence
	err = irods_fs.CloseDataObject(conn, handle)
	if err != nil {
		fs.session.ReturnConnection(conn)
		return nil, err
	}

	entry, err := fs.getDataObjectWithConnectionNoCache(conn, irodsPath)
	if err != nil {
		fs.session.ReturnConnection(conn)
		return nil, err
	}

	// re-open
	handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, mode)
	if err != nil {
		fs.session.ReturnConnection(conn)
		return nil, err
	}

	// do not return connection here
	fileHandle := &FileHandle{
		id:              xid.New().String(),
		filesystem:      fs,
		connection:      conn,
		irodsfilehandle: handle,
		entry:           entry,
		offset:          offset,
		openmode:        types.FileOpenMode(mode),
	}

	fs.fileHandleMap.Add(fileHandle)
	fs.invalidateCacheForFileCreate(irodsPath)
	fs.cachePropagation.PropagateFileCreate(irodsPath)

	return fileHandle, nil
}

// ClearCache clears all file system caches
func (fs *FileSystem) ClearCache() {
	fs.cache.ClearDirACLsCache()
	fs.cache.ClearFileACLsCache()
	fs.cache.ClearMetadataCache()
	fs.cache.ClearEntryCache()
	fs.cache.ClearNegativeEntryCache()
	fs.cache.ClearDirCache()
}

// getCollectionNoCache returns collection entry
func (fs *FileSystem) getCollectionNoCache(path string) (*Entry, error) {
	// retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collection, err := irods_fs.GetCollection(conn, path)
	if err != nil {
		return nil, err
	}

	if collection.ID > 0 {
		entry := fs.getEntryFromCollection(collection)

		// cache it
		fs.cache.RemoveNegativeEntryCache(path)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a directory")
}

// getCollection returns collection entry
func (fs *FileSystem) getCollection(path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, types.NewFileNotFoundErrorf("could not find a directory")
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == DirectoryEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getCollectionNoCache(path)
}

// getCollectionFromEntry returns collection from entry
func (fs *FileSystem) getCollectionFromEntry(entry *Entry) *types.IRODSCollection {
	return &types.IRODSCollection{
		ID:         entry.ID,
		Path:       entry.Path,
		Name:       entry.Name,
		Owner:      entry.Owner,
		CreateTime: entry.CreateTime,
		ModifyTime: entry.ModifyTime,
	}
}

func (fs *FileSystem) getEntryFromCollection(collection *types.IRODSCollection) *Entry {
	return &Entry{
		ID:         collection.ID,
		Type:       DirectoryEntry,
		Name:       collection.Name,
		Path:       collection.Path,
		Owner:      collection.Owner,
		Size:       0,
		CreateTime: collection.CreateTime,
		ModifyTime: collection.ModifyTime,
		CheckSum:   "",
	}
}

func (fs *FileSystem) getEntryFromDataObject(dataobject *types.IRODSDataObject) *Entry {
	return &Entry{
		ID:         dataobject.ID,
		Type:       FileEntry,
		Name:       dataobject.Name,
		Path:       dataobject.Path,
		Owner:      dataobject.Replicas[0].Owner,
		Size:       dataobject.Size,
		CreateTime: dataobject.Replicas[0].CreateTime,
		ModifyTime: dataobject.Replicas[0].ModifyTime,
		CheckSum:   dataobject.Replicas[0].CheckSum,
	}
}

// listEntries lists entries in a collection
func (fs *FileSystem) listEntries(collection *types.IRODSCollection) ([]*Entry, error) {
	// check cache first
	cachedEntries := []*Entry{}
	useCached := false

	cachedDirEntryPaths := fs.cache.GetDirCache(collection.Path)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedEntry := fs.cache.GetEntryCache(cachedDirEntryPath)
			if cachedEntry != nil {
				cachedEntries = append(cachedEntries, cachedEntry)
			} else {
				useCached = false
			}
		}
	}

	if useCached {
		// remove from nagative entry cache
		for _, cachedEntry := range cachedEntries {
			fs.cache.RemoveNegativeEntryCache(cachedEntry.Path)
		}
		return cachedEntries, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collections, err := irods_fs.ListSubCollections(conn, collection.Path)
	if err != nil {
		return nil, err
	}

	entries := []*Entry{}

	for _, coll := range collections {
		entry := fs.getEntryFromCollection(coll)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	dataobjects, err := irods_fs.ListDataObjectsMasterReplica(conn, collection)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		entry := fs.getEntryFromDataObject(dataobject)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	// cache dir entries
	dirEntryPaths := []string{}
	for _, entry := range entries {
		dirEntryPaths = append(dirEntryPaths, entry.Path)
	}
	fs.cache.AddDirCache(collection.Path, dirEntryPaths)

	return entries, nil
}

// searchEntriesByMeta searches entries by meta
func (fs *FileSystem) searchEntriesByMeta(metaName string, metaValue string) ([]*Entry, error) {
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collections, err := irods_fs.SearchCollectionsByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	entries := []*Entry{}

	for _, coll := range collections {
		entry := fs.getEntryFromCollection(coll)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	dataobjects, err := irods_fs.SearchDataObjectsMasterReplicaByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		entry := fs.getEntryFromDataObject(dataobject)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	return entries, nil
}

// getDataObjectWithConnectionNoCache returns an entry for data object
func (fs *FileSystem) getDataObjectWithConnectionNoCache(conn *connection.IRODSConnection, path string) (*Entry, error) {
	// retrieve it and add it to cache
	collectionEntry, err := fs.getCollection(util.GetIRODSPathDirname(path))
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	dataobject, err := irods_fs.GetDataObjectMasterReplica(conn, collection, util.GetIRODSPathFileName(path))
	if err != nil {
		return nil, err
	}

	if dataobject.ID > 0 {
		entry := fs.getEntryFromDataObject(dataobject)

		// cache it
		fs.cache.RemoveNegativeEntryCache(path)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a data object")
}

// getDataObjectWithConnection returns an entry for data object
func (fs *FileSystem) getDataObjectWithConnection(conn *connection.IRODSConnection, path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, types.NewFileNotFoundErrorf("could not find a data object")
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getDataObjectWithConnectionNoCache(conn, path)
}

// getDataObjectNoCache returns an entry for data object
func (fs *FileSystem) getDataObjectNoCache(path string) (*Entry, error) {
	// retrieve it and add it to cache
	collectionEntry, err := fs.getCollection(util.GetIRODSPathDirname(path))
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	dataobject, err := irods_fs.GetDataObjectMasterReplica(conn, collection, util.GetIRODSPathFileName(path))
	if err != nil {
		return nil, err
	}

	if dataobject.ID > 0 {
		entry := fs.getEntryFromDataObject(dataobject)

		// cache it
		fs.cache.RemoveNegativeEntryCache(path)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a data object")
}

// getDataObject returns an entry for data object
func (fs *FileSystem) getDataObject(path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, types.NewFileNotFoundErrorf("could not find a data object")
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getDataObjectNoCache(path)
}

// ListMetadata lists metadata for the given path
func (fs *FileSystem) ListMetadata(path string) ([]*types.IRODSMeta, error) {
	// check cache first
	cachedEntry := fs.cache.GetMetadataCache(path)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	irodsCorrectPath := util.GetCorrectIRODSPath(path)

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	var metadataobjects []*types.IRODSMeta

	if fs.ExistsDir(irodsCorrectPath) {
		metadataobjects, err = irods_fs.ListCollectionMeta(conn, irodsCorrectPath)
		if err != nil {
			return nil, err
		}
	} else {
		collectionEntry, err := fs.getCollection(util.GetIRODSPathDirname(path))
		if err != nil {
			return nil, err
		}

		collection := fs.getCollectionFromEntry(collectionEntry)

		metadataobjects, err = irods_fs.ListDataObjectMeta(conn, collection, util.GetIRODSPathFileName(irodsCorrectPath))
		if err != nil {
			return nil, err
		}
	}

	// cache it
	fs.cache.AddMetadataCache(irodsCorrectPath, metadataobjects)

	return metadataobjects, nil
}

// AddMetadata adds a metadata for the path
func (fs *FileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	if fs.ExistsDir(irodsCorrectPath) {
		err = irods_fs.AddCollectionMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	} else {
		err = irods_fs.AddDataObjectMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	}

	fs.cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// DeleteMetadata deletes a metadata for the path
func (fs *FileSystem) DeleteMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	if fs.ExistsDir(irodsCorrectPath) {
		err = irods_fs.DeleteCollectionMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	} else {
		err = irods_fs.DeleteDataObjectMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	}

	fs.cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// invalidateCacheForFileUpdate invalidates cache for update on the given file
func (fs *FileSystem) invalidateCacheForFileUpdate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)

	// modification doesn't affect to parent dir's modified time
}

// invalidateCacheForRemoveInternal invalidates cache for removal of the given file/dir
func (fs *FileSystem) invalidateCacheForRemoveInternal(path string, recurse bool) {
	var entry *Entry
	if recurse {
		entry = fs.cache.GetEntryCache(path)
	}

	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveFileACLsCache(path)
	fs.cache.RemoveMetadataCache(path)

	if recurse && entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCacheForRemoveInternal(dirEntry, recurse)
			}
		}
	}

	// remove dircache and dir acl cache even if it is a file or unknown, no harm.
	fs.cache.RemoveDirCache(path)
	fs.cache.RemoveDirACLsCache(path)
}

// invalidateCacheForDirCreate invalidates cache for creation of the given dir
func (fs *FileSystem) invalidateCacheForDirCreate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		parentDirEntries = append(parentDirEntries, path)
		fs.cache.AddDirCache(parentPath, parentDirEntries)
	}
}

// invalidateCacheForDirRemove invalidates cache for removal of the given dir
func (fs *FileSystem) invalidateCacheForDirRemove(path string, recurse bool) {
	var entry *Entry
	if recurse {
		entry = fs.cache.GetEntryCache(path)
	}

	// we need to expunge all negatie entry caches under irodsDestPath
	// since all sub-directories/files are also moved
	fs.cache.RemoveAllNegativeEntryCacheForPath(path)

	fs.cache.AddNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveMetadataCache(path)

	if recurse && entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCacheForRemoveInternal(dirEntry, recurse)
			}
		}
	}

	fs.cache.RemoveDirCache(path)
	fs.cache.RemoveDirACLsCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		newParentDirEntries := []string{}
		for _, dirEntry := range parentDirEntries {
			if dirEntry != path {
				newParentDirEntries = append(newParentDirEntries, dirEntry)
			}
		}
		fs.cache.AddDirCache(parentPath, newParentDirEntries)
	}
}

// invalidateCacheForFileCreate invalidates cache for creation of the given file
func (fs *FileSystem) invalidateCacheForFileCreate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		parentDirEntries = append(parentDirEntries, path)
		fs.cache.AddDirCache(parentPath, parentDirEntries)
	}
}

// invalidateCacheForFileRemove invalidates cache for removal of the given file
func (fs *FileSystem) invalidateCacheForFileRemove(path string) {
	fs.cache.AddNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveFileACLsCache(path)
	fs.cache.RemoveMetadataCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		newParentDirEntries := []string{}
		for _, dirEntry := range parentDirEntries {
			if dirEntry != path {
				newParentDirEntries = append(newParentDirEntries, dirEntry)
			}
		}
		fs.cache.AddDirCache(parentPath, newParentDirEntries)
	}
}

// AddUserMetadata adds a user metadata
func (fs *FileSystem) AddUserMetadata(user string, avuid int64, attName, attValue, attUnits string) error {
	metadata := &types.IRODSMeta{
		AVUID: avuid,
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.AddUserMeta(conn, user, metadata)
	if err != nil {
		return err
	}

	return nil
}

// DeleteUserMetadata deletes a user metadata
func (fs *FileSystem) DeleteUserMetadata(user string, avuid int64, attName, attValue, attUnits string) error {
	metadata := &types.IRODSMeta{
		AVUID: avuid,
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.session.ReturnConnection(conn)

	err = irods_fs.DeleteUserMeta(conn, user, metadata)
	if err != nil {
		return err
	}

	return nil
}

// ListUserMetadata lists all user metadata
func (fs *FileSystem) ListUserMetadata(user string) ([]*types.IRODSMeta, error) {
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	metadataobjects, err := irods_fs.ListUserMeta(conn, user)
	if err != nil {
		return nil, err
	}

	return metadataobjects, nil
}

// GetTicketForAnonymousAccess gets ticket information for anonymous access
func (fs *FileSystem) GetTicketForAnonymousAccess(ticket string) (*types.IRODSTicketForAnonymousAccess, error) {
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	ticketInfo, err := irods_fs.GetTicketForAnonymousAccess(conn, ticket)
	if err != nil {
		return nil, err
	}

	return ticketInfo, err
}
