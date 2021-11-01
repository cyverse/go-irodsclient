package fs

import (
	"fmt"
	"os"
	"sync"
	"time"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	Account     *types.IRODSAccount
	Config      *FileSystemConfig
	Session     *session.IRODSSession
	Cache       *FileSystemCache
	Mutex       sync.Mutex
	FileHandles map[string]*FileHandle
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
		Account:     account,
		Config:      config,
		Session:     sess,
		Cache:       cache,
		FileHandles: map[string]*FileHandle{},
	}, nil
}

// NewFileSystemWithDefault creates a new FileSystem with default configurations
func NewFileSystemWithDefault(account *types.IRODSAccount, applicationName string) (*FileSystem, error) {
	config := NewFileSystemConfigWithDefault(applicationName)
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime)

	return &FileSystem{
		Account:     account,
		Config:      config,
		Session:     sess,
		Cache:       cache,
		FileHandles: map[string]*FileHandle{},
	}, nil
}

// NewFileSystemWithSessionConfig creates a new FileSystem with custom session configurations
func NewFileSystemWithSessionConfig(account *types.IRODSAccount, sessConfig *session.IRODSSessionConfig) (*FileSystem, error) {
	config := NewFileSystemConfigWithDefault(sessConfig.ApplicationName)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		return nil, err
	}

	cache := NewFileSystemCache(config.CacheTimeout, config.CacheCleanupTime)

	return &FileSystem{
		Account:     account,
		Config:      config,
		Session:     sess,
		Cache:       cache,
		FileHandles: map[string]*FileHandle{},
	}, nil
}

// Release releases all resources
func (fs *FileSystem) Release() {
	handles := []*FileHandle{}

	// empty
	fs.Mutex.Lock()
	for _, handle := range fs.FileHandles {
		handles = append(handles, handle)
	}
	fs.FileHandles = map[string]*FileHandle{}
	fs.Mutex.Unlock()

	for _, handle := range handles {
		handle.closeWithoutFSHandleManagement()
	}

	fs.Session.Release()
}

// Connections counts current established connections
func (fs *FileSystem) Connections() int {
	return fs.Session.Connections()
}

// ListGroupUsers lists all users in a group
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

// ListGroups lists all groups
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

// ListUserGroups lists all groups that a user belongs to
func (fs *FileSystem) ListUserGroups(user string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.Cache.GetUserGroupsCache(user)
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

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
	fs.Cache.AddUserGroupsCache(user, groups)

	return groups, nil
}

// ListUsers lists all users
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
func (fs *FileSystem) Stat(path string) (*Entry, error) {
	// check if a cached Entry for the given path is a dir or a file
	if fs.isCacheDataObject(path) {
		fileStat, err := fs.StatFile(path)
		if err != nil {
			if !types.IsFileNotFoundError(err) {
				return nil, err
			}
		} else {
			return fileStat, nil
		}

		dirStat, err := fs.StatDir(path)
		if err != nil {
			if !types.IsFileNotFoundError(err) {
				return nil, err
			}
		} else {
			return dirStat, nil
		}
	} else {
		// default
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
	}

	// not a collection, not a data object
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
func (fs *FileSystem) List(path string) ([]*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	collection, err := fs.getCollection(irodsPath)
	if err != nil {
		return nil, err
	}

	return fs.listEntries(collection.Internal.(*types.IRODSCollection))
}

// SearchByMeta searches all file system entries with given metadata
func (fs *FileSystem) SearchByMeta(metaname string, metavalue string) ([]*Entry, error) {
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

	fs.invalidateCachePathRecursively(irodsPath)
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

	fs.invalidateCachePathRecursively(irodsPath)
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

	fs.invalidateCachePathRecursively(irodsSrcPath)
	fs.invalidateCachePathRecursively(irodsDestPath)
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

	fs.invalidateCachePathRecursively(irodsSrcPath)
	fs.invalidateCachePathRecursively(irodsDestPath)
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

	fs.invalidateCachePathRecursively(irodsPath)
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

	fs.invalidateCachePathRecursively(irodsDestPath)
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

	fs.invalidateCachePath(irodsPath)
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

	err = irods_fs.ReplicateDataObject(conn, irodsPath, resource, update, false)
	if err != nil {
		return err
	}

	fs.invalidateCachePath(irodsPath)
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

	return irods_fs.DownloadDataObject(fs.Session, irodsSrcPath, resource, localFilePath)
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

	return irods_fs.DownloadDataObjectParallel(fs.Session, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum)
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

	return irods_fs.DownloadDataObjectParallelInBlocksAsync(fs.Session, irodsSrcPath, resource, localFilePath, srcStat.Size, blockLength, taskNum)
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

	err = irods_fs.UploadDataObject(fs.Session, localSrcPath, irodsFilePath, resource, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCachePathRecursively(irodsFilePath)
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

	err = irods_fs.UploadDataObjectParallel(fs.Session, localSrcPath, irodsFilePath, resource, taskNum, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCachePathRecursively(irodsFilePath)
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

	outputChan2, errChan2 := irods_fs.UploadDataObjectParallelInBlockAsync(fs.Session, localSrcPath, irodsFilePath, resource, blockLength, taskNum, replicate)
	fs.invalidateCachePathRecursively(irodsFilePath)
	return outputChan2, errChan2
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

	var entry *Entry = nil
	if types.IsFileOpenFlagOpeningExisting(types.FileOpenMode(mode)) {
		// file may exists
		entryExisting, err := fs.StatFile(irodsPath)
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
			Owner:      fs.Account.ClientUser,
			Size:       0,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			CheckSum:   "",
			Internal:   nil,
		}
	}

	// do not return connection here
	fileHandle := &FileHandle{
		ID:          xid.New().String(),
		FileSystem:  fs,
		Connection:  conn,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      offset,
		OpenMode:    types.FileOpenMode(mode),
	}

	fs.Mutex.Lock()
	fs.FileHandles[fileHandle.ID] = fileHandle
	fs.Mutex.Unlock()

	return fileHandle, nil
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
	entry := &Entry{
		ID:         0,
		Type:       FileEntry,
		Name:       util.GetIRODSPathFileName(irodsPath),
		Path:       irodsPath,
		Owner:      fs.Account.ClientUser,
		Size:       0,
		CreateTime: time.Now(),
		ModifyTime: time.Now(),
		CheckSum:   "",
		Internal:   nil,
	}

	fileHandle := &FileHandle{
		ID:          xid.New().String(),
		FileSystem:  fs,
		Connection:  conn,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      0,
		OpenMode:    types.FileOpenModeWriteOnly,
	}

	fs.Mutex.Lock()
	fs.FileHandles[fileHandle.ID] = fileHandle
	fs.Mutex.Unlock()

	return fileHandle, nil
}

// ClearCache clears all file system caches
func (fs *FileSystem) ClearCache() {
	fs.Cache.ClearDirACLsCache()
	fs.Cache.ClearFileACLsCache()
	fs.Cache.ClearMetadataCache()
	fs.Cache.ClearEntryCache()
	fs.Cache.ClearDirCache()
}

// InvalidateCache invalidates cache with the given path
func (fs *FileSystem) InvalidateCache(path string) {
	irodsPath := util.GetCorrectIRODSPath(path)

	fs.invalidateCachePath(irodsPath)
}

// getCollection returns collection entry
func (fs *FileSystem) getCollection(path string) (*Entry, error) {
	// check cache first
	cachedEntry := fs.Cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == DirectoryEntry {
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
		entry := &Entry{
			ID:         collection.ID,
			Type:       DirectoryEntry,
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
		fs.Cache.AddEntryCache(entry)

		return entry, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a directory")
}

// listEntries lists entries in a collection
func (fs *FileSystem) listEntries(collection *types.IRODSCollection) ([]*Entry, error) {
	// check cache first
	cachedEntries := []*Entry{}
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

	entries := []*Entry{}

	for _, coll := range collections {
		entry := &Entry{
			ID:         coll.ID,
			Type:       DirectoryEntry,
			Name:       coll.Name,
			Path:       coll.Path,
			Owner:      coll.Owner,
			Size:       0,
			CreateTime: coll.CreateTime,
			ModifyTime: coll.ModifyTime,
			CheckSum:   "",
			Internal:   coll,
		}

		entries = append(entries, entry)

		// cache it
		fs.Cache.AddEntryCache(entry)
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

		entry := &Entry{
			ID:         dataobject.ID,
			Type:       FileEntry,
			Name:       dataobject.Name,
			Path:       dataobject.Path,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		entries = append(entries, entry)

		// cache it
		fs.Cache.AddEntryCache(entry)
	}

	// cache dir entries
	dirEntryPaths := []string{}
	for _, entry := range entries {
		dirEntryPaths = append(dirEntryPaths, entry.Path)
	}
	fs.Cache.AddDirCache(collection.Path, dirEntryPaths)

	return entries, nil
}

// searchEntriesByMeta searches entries by meta
func (fs *FileSystem) searchEntriesByMeta(metaName string, metaValue string) ([]*Entry, error) {
	conn, err := fs.Session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.Session.ReturnConnection(conn)

	collections, err := irods_fs.SearchCollectionsByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	entries := []*Entry{}

	for _, coll := range collections {
		entry := &Entry{
			ID:         coll.ID,
			Type:       DirectoryEntry,
			Name:       coll.Name,
			Path:       coll.Path,
			Owner:      coll.Owner,
			Size:       0,
			CreateTime: coll.CreateTime,
			ModifyTime: coll.ModifyTime,
			CheckSum:   "",
			Internal:   coll,
		}

		entries = append(entries, entry)

		// cache it
		fs.Cache.AddEntryCache(entry)
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

		entry := &Entry{
			ID:         dataobject.ID,
			Type:       FileEntry,
			Name:       dataobject.Name,
			Path:       dataobject.Path,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		entries = append(entries, entry)

		// cache it
		fs.Cache.AddEntryCache(entry)
	}

	return entries, nil
}

// isCacheDataObject checks if given path is for data object, return false if unknown
func (fs *FileSystem) isCacheDataObject(path string) bool {
	// check cache
	cachedEntry := fs.Cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return true
	}
	return false
}

// getDataObject returns an entry for data object
func (fs *FileSystem) getDataObject(path string) (*Entry, error) {
	// check cache first
	cachedEntry := fs.Cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
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
		entry := &Entry{
			ID:         dataobject.ID,
			Type:       FileEntry,
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
		fs.Cache.AddEntryCache(entry)

		return entry, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a data object")
}

// ListMetadata lists metadata for the given path
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

// AddMetadata adds a metadata for the path
func (fs *FileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

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

	fs.Cache.RemoveMetadataCache(irodsCorrectPath)
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

	fs.Cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// invalidateCachePath invalidates cache with the given path
func (fs *FileSystem) invalidateCachePath(path string) {
	fs.Cache.RemoveEntryCache(path)
	fs.Cache.RemoveDirCache(path)
	fs.Cache.RemoveFileACLsCache(path)
	fs.Cache.RemoveDirACLsCache(path)
	fs.Cache.RemoveMetadataCache(path)
}

func (fs *FileSystem) invalidateCachePathRecursively(path string) {
	// if path is directory, recursively
	entry := fs.Cache.GetEntryCache(path)
	fs.Cache.RemoveEntryCache(path)
	fs.Cache.RemoveFileACLsCache(path)
	fs.Cache.RemoveMetadataCache(path)

	if entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.Cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCachePathRecursively(dirEntry)
			}
		}

		fs.Cache.RemoveDirCache(path)
		fs.Cache.RemoveDirACLsCache(path)

		fs.Cache.RemoveDirCache(util.GetIRODSPathDirname(path))
		fs.Cache.RemoveDirACLsCache(util.GetIRODSPathDirname(path))
	} else {
		fs.Cache.RemoveDirCache(path)
		fs.Cache.RemoveDirACLsCache(path)
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

// DeleteUserMetadata deletes a user metadata
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

// ListUserMetadata lists all user metadata
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
