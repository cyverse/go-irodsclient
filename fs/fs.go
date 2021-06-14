package fs

import (
	"fmt"
	"os"
	"time"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	Account *types.IRODSAccount
	Config  *FileSystemConfig
	Session *session.IRODSSession
	Cache   *FileSystemCache
}

// NewFileSystem creates a new FileSystem
func NewFileSystem(account *types.IRODSAccount, config *FileSystemConfig) (*FileSystem, error) {
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime)

	return &FileSystem{
		Account: account,
		Config:  config,
		Session: sess,
		Cache:   cache,
	}, nil
}

// NewFileSystemWithDefault ...
func NewFileSystemWithDefault(account *types.IRODSAccount, applicationName string) (*FileSystem, error) {
	config := NewFileSystemConfigWithDefault(applicationName)
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime)

	return &FileSystem{
		Account: account,
		Config:  config,
		Session: sess,
		Cache:   cache,
	}, nil
}

// Release ...
func (fs *FileSystem) Release() {
	fs.Session.Release()
}

func (fs *FileSystem) ListGroupUsers(group string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.Cache.GetGroupUsersCache(group)
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	users, err := irods_fs.ListGroupUsers(conn, group)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.Cache.AddGroupUsersCache(group, users)

	return users, nil
}

func (fs *FileSystem) ListGroups() ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.Cache.GetGroupsCache()
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	groups, err := irods_fs.ListGroups(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.Cache.AddGroupsCache(groups)

	return groups, nil
}

func (fs *FileSystem) ListUsers() ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.Cache.GetUsersCache()
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	users, err := irods_fs.ListUsers(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.Cache.AddUsersCache(users)

	return users, nil
}

// Stat returns file status
func (fs *FileSystem) Stat(path string) (*FSEntry, error) {
	dirStat, err := fs.StatDir(path)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return dirStat, nil
	}

	fileStat, err := fs.StatFile(path)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return fileStat, nil
	}

	// not a collection, not a data object
	return nil, types.NewFileNotFoundError("Could not find a data object or a directory")
}

// StatDir returns status of a directory
func (fs *FileSystem) StatDir(path string) (*FSEntry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	return fs.getCollection(irodsPath)
}

// StatFile returns status of a file
func (fs *FileSystem) StatFile(path string) (*FSEntry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	return fs.getDataObject(irodsPath)
}

// Exists checks file/directory existance
func (fs *FileSystem) Exists(path string) bool {
	entry, err := fs.Stat(path)
	if err != nil {
		return false
	}
	if entry.ID > 0 {
		return true
	}
	return false
}

// ExistsDir checks directory existance
func (fs *FileSystem) ExistsDir(path string) bool {
	entry, err := fs.StatDir(path)
	if err != nil {
		return false
	}
	if entry.ID > 0 {
		return true
	}
	return false
}

// ExistsFile checks file existance
func (fs *FileSystem) ExistsFile(path string) bool {
	entry, err := fs.StatFile(path)
	if err != nil {
		return false
	}
	if entry.ID > 0 {
		return true
	}
	return false
}

// ListACLs returns ACLs
func (fs *FileSystem) ListACLs(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	if stat.Type == FSDirectoryEntry {
		return fs.ListDirACLs(path)
	} else if stat.Type == FSFileEntry {
		return fs.ListFileACLs(path)
	} else {
		return nil, fmt.Errorf("Unknown type - %s", stat.Type)
	}
}

// ListACLsWithGroupUsers returns ACLs
func (fs *FileSystem) ListACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}
	if stat.Type == FSDirectoryEntry {
		accessList, err := fs.ListDirACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else if stat.Type == FSFileEntry {
		accessList, err := fs.ListFileACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else {
		return nil, fmt.Errorf("Unknown type - %s", stat.Type)
	}

	return accesses, nil
}

// ListDirACLs returns ACLs of a directory
func (fs *FileSystem) ListDirACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.Cache.GetDirACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	accesses, err := irods_fs.ListCollectionAccess(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.Cache.AddDirACLsCache(irodsPath, accesses)

	return accesses, nil
}

// ListDirACLsWithGroupUsers returns ACLs of a directory
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
	cachedAccesses := fs.Cache.GetFileACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	collection, err := fs.getCollection(util.GetIRODSPathDirname(irodsPath))
	if err != nil {
		return nil, err
	}

	accesses, err := irods_fs.ListDataObjectAccess(conn, collection.Internal.(*types.IRODSCollection), util.GetIRODSPathFileName(irodsPath))
	if err != nil {
		return nil, err
	}

	// cache it
	fs.Cache.AddFileACLsCache(irodsPath, accesses)

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
func (fs *FileSystem) List(path string) ([]*FSEntry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	collection, err := fs.getCollection(irodsPath)
	if err != nil {
		return nil, err
	}

	return fs.listEntries(collection.Internal.(*types.IRODSCollection))
}

// SearchByMeta searches all file system entries with given metadata
func (fs *FileSystem) SearchByMeta(metaname string, metavalue string) ([]*FSEntry, error) {
	return fs.searchEntriesByMeta(metaname, metavalue)
}

// RemoveDir deletes a directory
func (fs *FileSystem) RemoveDir(path string, recurse bool, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.DeleteCollection(conn, irodsPath, recurse, force)
	if err != nil {
		return err
	}

	fs.removeCachePath(irodsPath)
	return nil
}

// RemoveFile deletes a file
func (fs *FileSystem) RemoveFile(path string, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.DeleteDataObject(conn, irodsPath, force)
	if err != nil {
		return err
	}

	fs.removeCachePath(irodsPath)
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

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.MoveCollection(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	if util.GetIRODSPathDirname(irodsSrcPath) == util.GetIRODSPathDirname(irodsDestPath) {
		// from the same dir
		fs.invalidateCachePath(util.GetIRODSPathDirname(irodsSrcPath))

	} else {
		fs.removeCachePath(irodsSrcPath)
		fs.invalidateCachePath(util.GetIRODSPathDirname(irodsDestPath))
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

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.MoveDataObject(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	if util.GetIRODSPathDirname(irodsSrcPath) == util.GetIRODSPathDirname(irodsDestPath) {
		// from the same dir
		fs.invalidateCachePath(util.GetIRODSPathDirname(irodsSrcPath))
	} else {
		fs.removeCachePath(irodsSrcPath)
		fs.invalidateCachePath(util.GetIRODSPathDirname(irodsDestPath))
	}

	return nil
}

// MakeDir creates a directory
func (fs *FileSystem) MakeDir(path string, recurse bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.CreateCollection(conn, irodsPath, recurse)
	if err != nil {
		return err
	}

	fs.invalidateCachePath(util.GetIRODSPathDirname(irodsPath))

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

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.CopyDataObject(conn, irodsSrcPath, irodsDestPath)
	if err != nil {
		return err
	}

	fs.invalidateCachePath(util.GetIRODSPathDirname(irodsDestPath))

	return nil
}

// TruncateFile truncates a file
func (fs *FileSystem) TruncateFile(path string, size int64) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	if size < 0 {
		size = 0
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.TruncateDataObject(conn, irodsPath, size)
	if err != nil {
		return err
	}

	fs.invalidateCachePath(util.GetIRODSPathDirname(irodsPath))

	return nil
}

// ReplicateFile replicates a file
func (fs *FileSystem) ReplicateFile(path string, resource string, update bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	return irods_fs.ReplicateDataObject(conn, irodsPath, resource, update, false)
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, localPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath
	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			localFilePath = localDestPath
		} else {
			return err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			return fmt.Errorf("File %s already exists", localDestPath)
		}
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	return irods_fs.DownloadDataObject(conn, irodsSrcPath, localFilePath)
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
			return types.NewFileNotFoundError("Could not find the local file")
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
		case FSFileEntry:
			// do nothing
		case FSDirectoryEntry:
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fmt.Errorf("Unknown entry type %s", entry.Type)
		}
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.UploadDataObject(conn, localSrcPath, irodsFilePath, resource, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCachePath(util.GetIRODSPathDirname(irodsFilePath))

	return nil
}

// OpenFile opens an existing file for read/write
func (fs *FileSystem) OpenFile(path string, resource string, mode string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}

	handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, mode)
	if err != nil {
		fs.Session.ReturnConnection(conn)
		return nil, err
	}

	var entry *FSEntry = nil
	if types.IsFileOpenFlagOpeningExisting(types.FileOpenMode(mode)) {
		// file may exists
		entryExisting, err := fs.StatFile(irodsPath)
		if err == nil {
			entry = entryExisting
		}
	}

	if entry == nil {
		// create a new
		entry = &FSEntry{
			ID:         0,
			Type:       FSFileEntry,
			Name:       util.GetIRODSPathFileName(irodsPath),
			Path:       irodsPath,
			Owner:      fs.Account.ClientUser,
			Size:       0,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			CheckSum:   "",
			Internal:   nil,
		}
	}

	// do not return connection here
	return &FileHandle{
		FileSystem:  fs,
		Connection:  conn,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      offset,
		OpenMode:    types.FileOpenMode(mode),
	}, nil
}

// CreateFile opens a new file for write
func (fs *FileSystem) CreateFile(path string, resource string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}

	handle, err := irods_fs.CreateDataObject(conn, irodsPath, resource, true)
	if err != nil {
		fs.Session.ReturnConnection(conn)
		return nil, err
	}

	// do not return connection here
	entry := &FSEntry{
		ID:         0,
		Type:       FSFileEntry,
		Name:       util.GetIRODSPathFileName(irodsPath),
		Path:       irodsPath,
		Owner:      fs.Account.ClientUser,
		Size:       0,
		CreateTime: time.Now(),
		ModifyTime: time.Now(),
		CheckSum:   "",
		Internal:   nil,
	}

	return &FileHandle{
		FileSystem:  fs,
		Connection:  conn,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      0,
		OpenMode:    types.FileOpenModeWriteOnly,
	}, nil
}

// ClearCache ...
func (fs *FileSystem) ClearCache() {
	fs.Cache.ClearEntryCache()
	fs.Cache.ClearDirCache()
}

// InvalidateCache invalidates cache with the given path
func (fs *FileSystem) InvalidateCache(path string) {
	irodsPath := util.GetCorrectIRODSPath(path)

	fs.invalidateCachePath(irodsPath)
}

func (fs *FileSystem) getCollection(path string) (*FSEntry, error) {
	// check cache first
	cachedEntry := fs.Cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FSDirectoryEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	collection, err := irods_fs.GetCollection(conn, path)
	if err != nil {
		return nil, err
	}

	if collection.ID > 0 {
		fsEntry := &FSEntry{
			ID:         collection.ID,
			Type:       FSDirectoryEntry,
			Name:       collection.Name,
			Path:       collection.Path,
			Owner:      collection.Owner,
			Size:       0,
			CreateTime: collection.CreateTime,
			ModifyTime: collection.ModifyTime,
			CheckSum:   "",
			Internal:   collection,
		}

		// cache it
		fs.Cache.AddEntryCache(fsEntry)

		return fsEntry, nil
	}

	return nil, types.NewFileNotFoundErrorf("Could not find a directory")
}

func (fs *FileSystem) listEntries(collection *types.IRODSCollection) ([]*FSEntry, error) {
	// check cache first
	cachedEntries := []*FSEntry{}
	useCached := false

	cachedDirEntryPaths := fs.Cache.GetDirCache(collection.Path)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedEntry := fs.Cache.GetEntryCache(cachedDirEntryPath)
			if cachedEntry != nil {
				cachedEntries = append(cachedEntries, cachedEntry)
			} else {
				useCached = false
			}
		}
	}

	if useCached {
		return cachedEntries, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	collections, err := irods_fs.ListSubCollections(conn, collection.Path)
	if err != nil {
		return nil, err
	}

	fsEntries := []*FSEntry{}

	for _, coll := range collections {
		fsEntry := &FSEntry{
			ID:         coll.ID,
			Type:       FSDirectoryEntry,
			Name:       coll.Name,
			Path:       coll.Path,
			Owner:      coll.Owner,
			Size:       0,
			CreateTime: coll.CreateTime,
			ModifyTime: coll.ModifyTime,
			CheckSum:   "",
			Internal:   coll,
		}

		fsEntries = append(fsEntries, fsEntry)

		// cache it
		fs.Cache.AddEntryCache(fsEntry)
	}

	dataobjects, err := irods_fs.ListDataObjectsMasterReplica(conn, collection)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		replica := dataobject.Replicas[0]

		fsEntry := &FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Path:       dataobject.Path,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		fsEntries = append(fsEntries, fsEntry)

		// cache it
		fs.Cache.AddEntryCache(fsEntry)
	}

	// cache dir entries
	dirEntryPaths := []string{}
	for _, fsEntry := range fsEntries {
		dirEntryPaths = append(dirEntryPaths, fsEntry.Path)
	}
	fs.Cache.AddDirCache(collection.Path, dirEntryPaths)

	return fsEntries, nil
}

func (fs *FileSystem) searchEntriesByMeta(metaName string, metaValue string) ([]*FSEntry, error) {
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	collections, err := irods_fs.SearchCollectionsByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	fsEntries := []*FSEntry{}

	for _, coll := range collections {
		fsEntry := &FSEntry{
			ID:         coll.ID,
			Type:       FSDirectoryEntry,
			Name:       coll.Name,
			Path:       coll.Path,
			Owner:      coll.Owner,
			Size:       0,
			CreateTime: coll.CreateTime,
			ModifyTime: coll.ModifyTime,
			CheckSum:   "",
			Internal:   coll,
		}

		fsEntries = append(fsEntries, fsEntry)

		// cache it
		fs.Cache.AddEntryCache(fsEntry)
	}

	dataobjects, err := irods_fs.SearchDataObjectsMasterReplicaByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		replica := dataobject.Replicas[0]

		fsEntry := &FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Path:       dataobject.Path,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		fsEntries = append(fsEntries, fsEntry)

		// cache it
		fs.Cache.AddEntryCache(fsEntry)
	}

	return fsEntries, nil
}

func (fs *FileSystem) getDataObject(path string) (*FSEntry, error) {
	// check cache first
	cachedEntry := fs.Cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FSFileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	collection, err := fs.getCollection(util.GetIRODSPathDirname(path))
	if err != nil {
		return nil, err
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	dataobject, err := irods_fs.GetDataObjectMasterReplica(conn, collection.Internal.(*types.IRODSCollection), util.GetIRODSPathFileName(path))
	if err != nil {
		return nil, err
	}

	if dataobject.ID > 0 {
		fsEntry := &FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Path:       dataobject.Path,
			Owner:      dataobject.Replicas[0].Owner,
			Size:       dataobject.Size,
			CreateTime: dataobject.Replicas[0].CreateTime,
			ModifyTime: dataobject.Replicas[0].ModifyTime,
			CheckSum:   dataobject.Replicas[0].CheckSum,
			Internal:   dataobject,
		}

		// cache it
		fs.Cache.AddEntryCache(fsEntry)

		return fsEntry, nil
	}

	return nil, types.NewFileNotFoundErrorf("Could not find a data object")
}

func (fs *FileSystem) ListMetadata(path string) ([]*types.IRODSMeta, error) {
	// check cache first
	cachedEntry := fs.Cache.GetMetadataCache(path)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	irodsCorrectPath := util.GetCorrectIRODSPath(path)

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	var metadataobjects []*types.IRODSMeta

	if fs.ExistsDir(irodsCorrectPath) {
		metadataobjects, err = irods_fs.ListCollectionMeta(conn, irodsCorrectPath)
		if err != nil {
			return nil, err
		}
	} else {
		collection, err := fs.getCollection(util.GetIRODSPathDirname(path))
		if err != nil {
			return nil, err
		}
		metadataobjects, err = irods_fs.ListDataObjectMeta(conn, collection.Internal.(*types.IRODSCollection), util.GetIRODSPathFileName(irodsCorrectPath))
		if err != nil {
			return nil, err
		}
	}

	// cache it
	fs.Cache.AddMetadataCache(irodsCorrectPath, metadataobjects)

	return metadataobjects, nil
}

func (fs *FileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	fs.Cache.RemoveMetadataCache(irodsCorrectPath)

	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

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

	return nil
}

func (fs *FileSystem) DeleteMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	fs.Cache.RemoveMetadataCache(irodsCorrectPath)

	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

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

	return nil
}

// InvalidateCachePath invalidates cache with the given path
func (fs *FileSystem) invalidateCachePath(path string) {
	fs.Cache.RemoveEntryCache(path)
	fs.Cache.RemoveDirCache(path)
}

func (fs *FileSystem) removeCachePath(path string) {
	// if path is directory, recursively
	entry := fs.Cache.GetEntryCache(path)
	if entry != nil {
		fs.Cache.RemoveEntryCache(path)

		if entry.Type == FSDirectoryEntry {
			dirEntries := fs.Cache.GetDirCache(path)
			if dirEntries != nil {
				for _, dirEntry := range dirEntries {
					// do it recursively
					fs.removeCachePath(dirEntry)
				}
				fs.Cache.RemoveDirCache(path)
			}
		}

		fs.Cache.RemoveDirCache(util.GetIRODSPathDirname(path))
	}
}

func (fs *FileSystem) AddUserMetadata(user string, avuid int64, attName, attValue, attUnits string) error {
	metadata := &types.IRODSMeta{
		AVUID: avuid,
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.AddUserMeta(conn, user, metadata)
	if err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) DeleteUserMetadata(user string, avuid int64, attName, attValue, attUnits string) error {
	metadata := &types.IRODSMeta{
		AVUID: avuid,
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.Session.ReturnConnection(conn)

	err = irods_fs.DeleteUserMeta(conn, user, metadata)
	if err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) ListUserMetadata(user string) ([]*types.IRODSMeta, error) {
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	metadataobjects, err := irods_fs.ListUserMeta(conn, user)
	if err != nil {
		return nil, err
	}

	return metadataobjects, nil
}
