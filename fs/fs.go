package fs

import (
	"path"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
	"golang.org/x/xerrors"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	id                   string
	account              *types.IRODSAccount
	config               *FileSystemConfig
	ioSession            *session.IRODSSession
	metadataSession      *session.IRODSSession
	cache                *FileSystemCache
	cachePropagation     *FileSystemCachePropagation
	cacheEventHandlerMap *FilesystemCacheEventHandlerMap
	fileHandleMap        *FileHandleMap
}

// NewFileSystem creates a new FileSystem
func NewFileSystem(account *types.IRODSAccount, config *FileSystemConfig) (*FileSystem, error) {
	ioSessionConfig := config.ToIOSessionConfig()
	ioSession, err := session.NewIRODSSession(account, ioSessionConfig)
	if err != nil {
		return nil, err
	}

	metadataSessionConfig := config.ToMetadataSessionConfig()
	metaSession, err := session.NewIRODSSession(account, metadataSessionConfig)
	if err != nil {
		return nil, err
	}

	ioTransactionFailureHandler := func(commitFail bool, poormansRollbackFail bool) {
		metaSession.SetCommitFail(commitFail)
		metaSession.SetPoormansRollbackFail(poormansRollbackFail)
	}

	metaTransactionFailureHandler := func(commitFail bool, poormansRollbackFail bool) {
		ioSession.SetCommitFail(commitFail)
		ioSession.SetPoormansRollbackFail(poormansRollbackFail)
	}

	ioSession.SetTransactionFailureHandler(ioTransactionFailureHandler)
	metaSession.SetTransactionFailureHandler(metaTransactionFailureHandler)

	fs := &FileSystem{
		id:                   xid.New().String(), // generate a new ID
		account:              account,
		config:               config,
		ioSession:            ioSession,
		metadataSession:      metaSession,
		cache:                NewFileSystemCache(&config.Cache),
		cacheEventHandlerMap: NewFilesystemCacheEventHandlerMap(),
		fileHandleMap:        NewFileHandleMap(),
	}

	cachePropagation := NewFileSystemCachePropagation(fs)
	fs.cachePropagation = cachePropagation

	return fs, nil
}

// NewFileSystemWithDefault creates a new FileSystem with default configurations
func NewFileSystemWithDefault(account *types.IRODSAccount, applicationName string) (*FileSystem, error) {
	config := NewFileSystemConfig(applicationName)
	return NewFileSystem(account, config)
}

// Release releases all resources
func (fs *FileSystem) Release() {
	handles := fs.fileHandleMap.PopAll()
	for _, handle := range handles {
		handle.Close()
	}

	fs.cacheEventHandlerMap.Release()
	fs.cachePropagation.Release()

	fs.ioSession.Release()
	fs.metadataSession.Release()
}

// GetID returns file system instance ID
func (fs *FileSystem) GetID() string {
	return fs.id
}

// GetAccount returns IRODS account
func (fs *FileSystem) GetAccount() *types.IRODSAccount {
	return fs.account
}

// GetConfig returns file system config
func (fs *FileSystem) GetConfig() *FileSystemConfig {
	return fs.config
}

// GetIOConnection returns irods connection for IO
func (fs *FileSystem) GetIOConnection() (*connection.IRODSConnection, error) {
	return fs.ioSession.AcquireConnection()
}

// ReturnIOConnection returns irods connection for IO back to session
func (fs *FileSystem) ReturnIOConnection(conn *connection.IRODSConnection) error {
	return fs.ioSession.ReturnConnection(conn)
}

// GetMetadataConnection returns irods connection for metadata operations
func (fs *FileSystem) GetMetadataConnection() (*connection.IRODSConnection, error) {
	return fs.metadataSession.AcquireConnection()
}

// ReturnMetadataConnection returns irods connection for metadata operations back to session
func (fs *FileSystem) ReturnMetadataConnection(conn *connection.IRODSConnection) error {
	return fs.metadataSession.ReturnConnection(conn)
}

// ConnectionTotal counts current established connections
func (fs *FileSystem) ConnectionTotal() int {
	return fs.ioSession.ConnectionTotal() + fs.metadataSession.ConnectionTotal()
}

// GetServerVersion returns server version info
func (fs *FileSystem) GetServerVersion() (*types.IRODSVersion, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	return conn.GetVersion(), nil
}

// GetHomeDirPath returns the home directory path
func (fs *FileSystem) GetHomeDirPath() string {
	return fs.account.GetHomeDirPath()
}

// SupportParallelUpload returns if the server supports parallel upload
func (fs *FileSystem) SupportParallelUpload() bool {
	return fs.metadataSession.SupportParallelUpload()
}

// IsTicketAccess returns if the access is authenticated using ticket
func (fs *FileSystem) IsTicketAccess() bool {
	return fs.account.UseTicket()
}

// GetMetrics returns metrics
func (fs *FileSystem) GetMetrics() *metrics.IRODSMetrics {
	ioMetrics := fs.ioSession.GetMetrics()
	metaMetrics := fs.metadataSession.GetMetrics()

	newMetrics := &metrics.IRODSMetrics{}
	newMetrics.Sum(ioMetrics)
	newMetrics.Sum(metaMetrics)
	return newMetrics
}

// Stat returns file status
func (fs *FileSystem) Stat(irodsPath string) (*Entry, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	// check if a negative cache for the given path exists
	if fs.cache.HasNegativeEntryCache(irodsCorrectPath) {
		// has a negative cache - fail fast
		return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsCorrectPath, types.NewFileNotFoundError(irodsCorrectPath))
	}

	// check if a cached Entry for the given path exists
	cachedEntry := fs.cache.GetEntryCache(irodsCorrectPath)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// check if a cached dir Entry for the given path exists
	parentPath := path.Dir(irodsCorrectPath)
	cachedDirEntryPaths := fs.cache.GetDirCache(parentPath)
	dirEntryExist := false
	if cachedDirEntryPaths != nil {
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			if cachedDirEntryPath == irodsCorrectPath {
				dirEntryExist = true
				break
			}
		}

		if !dirEntryExist {
			// dir entry not exist - fail fast
			fs.cache.AddNegativeEntryCache(irodsCorrectPath)
			return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsCorrectPath, types.NewFileNotFoundError(irodsCorrectPath))
		}
	}

	// if cache does not exist,
	// check dir first
	dirStat, err := fs.getCollectionNoCache(irodsCorrectPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return dirStat, nil
	}

	// if it's not dir, check file
	fileStat, err := fs.getDataObjectNoCache(irodsCorrectPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return nil, err
		}
	} else {
		return fileStat, nil
	}

	// not a collection, not a data object
	fs.cache.AddNegativeEntryCache(irodsCorrectPath)
	return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsCorrectPath, types.NewFileNotFoundError(irodsCorrectPath))
}

// StatDir returns status of a directory
func (fs *FileSystem) StatDir(irodsPath string) (*Entry, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	return fs.getCollection(irodsCorrectPath)
}

// StatFile returns status of a file
func (fs *FileSystem) StatFile(irodsPath string) (*Entry, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	return fs.getDataObject(irodsCorrectPath)
}

// Exists checks file/directory existence
func (fs *FileSystem) Exists(irodsPath string) bool {
	entry, err := fs.Stat(irodsPath)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// ExistsDir checks directory existence
func (fs *FileSystem) ExistsDir(irodsPath string) bool {
	entry, err := fs.StatDir(irodsPath)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// ExistsFile checks file existence
func (fs *FileSystem) ExistsFile(irodsPath string) bool {
	entry, err := fs.StatFile(irodsPath)
	if err != nil {
		return false
	}
	return entry.ID > 0
}

// List lists all file system entries under the given path
func (fs *FileSystem) List(irodsPath string) ([]*Entry, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)
	return fs.listEntries(irodsCorrectPath)
}

func (fs *FileSystem) SearchUnixWildcard(pathUnixWildcard string) ([]*Entry, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	results := []*Entry{}

	collEntries, err := irods_fs.SearchCollectionsUnixWildcard(conn, pathUnixWildcard)
	if err != nil {
		return nil, err
	}

	for _, entry := range collEntries {
		results = append(results, NewEntryFromCollection(entry))
	}

	objectEntries, err := irods_fs.SearchDataObjectsMasterReplicaUnixWildcard(conn, pathUnixWildcard)
	if err != nil {
		return nil, err
	}

	for _, entry := range objectEntries {
		results = append(results, NewEntryFromDataObject(entry))
	}

	return results, nil
}

func (fs *FileSystem) SearchDirUnixWildcard(pathUnixWildcard string) ([]*Entry, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	results := []*Entry{}

	collEntries, err := irods_fs.SearchCollectionsUnixWildcard(conn, pathUnixWildcard)
	if err != nil {
		return nil, err
	}

	for _, entry := range collEntries {
		results = append(results, NewEntryFromCollection(entry))
	}

	return results, nil
}

func (fs *FileSystem) SearchFileUnixWildcard(pathUnixWildcard string) ([]*Entry, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	results := []*Entry{}

	objectEntries, err := irods_fs.SearchDataObjectsMasterReplicaUnixWildcard(conn, pathUnixWildcard)
	if err != nil {
		return nil, err
	}

	for _, entry := range objectEntries {
		results = append(results, NewEntryFromDataObject(entry))
	}

	return results, nil
}

// RemoveDir deletes a directory
func (fs *FileSystem) RemoveDir(irodsPath string, recurse bool, force bool) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteCollection(conn, irodsCorrectPath, recurse, force)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			fs.invalidateCacheForFileRemove(irodsCorrectPath)
			fs.cachePropagation.PropagateFileRemove(irodsCorrectPath)
		}
		return err
	}

	fs.invalidateCacheForDirRemove(irodsCorrectPath, recurse)
	fs.cachePropagation.PropagateDirRemove(irodsCorrectPath)
	return nil
}

// RemoveFile deletes a file
func (fs *FileSystem) RemoveFile(irodsPath string, force bool) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	// if file handle is opened, wg
	wg := util.NewTimeoutWaitGroup()
	wg.Add(1)

	eventHandlerID := fs.fileHandleMap.AddCloseEventHandler(irodsCorrectPath, func(path, id string, empty bool) {
		if empty {
			wg.Done()
		}
	})

	defer fs.fileHandleMap.RemoveCloseEventHandler(eventHandlerID)

	if !wg.WaitTimeout(time.Duration(fs.config.MetadataConnection.OperationTimeout)) {
		// timeout
		return xerrors.Errorf("failed to remove file, there are files still opened")
	}

	// wait done
	err = irods_fs.DeleteDataObject(conn, irodsCorrectPath, force)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			fs.invalidateCacheForFileRemove(irodsCorrectPath)
			fs.cachePropagation.PropagateFileRemove(irodsCorrectPath)
		}
		return err
	}

	fs.invalidateCacheForFileRemove(irodsCorrectPath)
	fs.cachePropagation.PropagateFileRemove(irodsCorrectPath)
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

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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
			relPath, err := util.GetRelativeIRODSPath(srcPath, handle.entry.Path)
			if err != nil {
				errs = append(errs, err)
			} else {
				destFullPath := path.Join(destPath, relPath)
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
func (fs *FileSystem) MakeDir(irodsPath string, recurse bool) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	dirEntry, err := fs.StatDir(irodsPath)
	if err == nil {
		if dirEntry.ID > 0 {
			// already exists
			if recurse {
				return nil
			}
			return types.NewFileAlreadyExistError(irodsPath)
		}
	}

	err = irods_fs.CreateCollection(conn, irodsCorrectPath, recurse)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirCreate(irodsCorrectPath)
	fs.cachePropagation.PropagateDirCreate(irodsCorrectPath)
	fs.cache.AddDirCache(irodsCorrectPath, []string{})
	return nil
}

// CopyFile copies a file
func (fs *FileSystem) CopyFile(srcPath string, destPath string, force bool) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	destFilePath := irodsDestPath
	if fs.ExistsDir(irodsDestPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(irodsSrcPath)
		destFilePath = util.MakeIRODSPath(irodsDestPath, srcFileName)
	}

	return fs.CopyFileToFile(irodsSrcPath, destFilePath, force)
}

// CopyFileToFile copies a file
func (fs *FileSystem) CopyFileToFile(srcPath string, destPath string, force bool) error {
	irodsSrcPath := util.GetCorrectIRODSPath(srcPath)
	irodsDestPath := util.GetCorrectIRODSPath(destPath)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.CopyDataObject(conn, irodsSrcPath, irodsDestPath, force)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsDestPath)
	fs.cachePropagation.PropagateFileCreate(irodsDestPath)
	return nil
}

// TruncateFile truncates a file
func (fs *FileSystem) TruncateFile(irodsPath string, size int64) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	if size < 0 {
		size = 0
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.TruncateDataObject(conn, irodsCorrectPath, size)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsCorrectPath)
	fs.cachePropagation.PropagateFileUpdate(irodsCorrectPath)
	return nil
}

// ReplicateFile replicates a file
func (fs *FileSystem) ReplicateFile(irodsPath string, resource string, update bool) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ReplicateDataObject(conn, irodsCorrectPath, resource, update, false)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsCorrectPath)
	fs.cachePropagation.PropagateFileUpdate(irodsCorrectPath)
	return nil
}

// OpenFile opens an existing file for read/write
func (fs *FileSystem) OpenFile(irodsPath string, resource string, mode string) (*FileHandle, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	keywords := map[common.KeyWord]string{}
	handle, offset, err := irods_fs.OpenDataObject(conn, irodsCorrectPath, resource, mode, keywords)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	var entry *Entry = nil
	openMode := types.FileOpenMode(mode)
	if openMode.IsOpeningExisting() {
		// file may exists
		// we don't use cache to use fresh data object info
		entryExisting, err := fs.getDataObjectWithConnectionNoCache(conn, irodsCorrectPath)
		if err == nil {
			entry = entryExisting
		}
	}

	if entry == nil {
		// create a new
		entry = &Entry{
			ID:                0,
			Type:              FileEntry,
			Name:              util.GetIRODSPathFileName(irodsCorrectPath),
			Path:              irodsCorrectPath,
			Owner:             fs.account.ClientUser,
			Size:              0,
			CreateTime:        time.Now(),
			ModifyTime:        time.Now(),
			CheckSumAlgorithm: types.ChecksumAlgorithmUnknown,
			CheckSum:          nil,
		}
	}

	// do not return connection here
	fileHandle := &FileHandle{
		id:              xid.New().String(),
		filesystem:      fs,
		connection:      conn,
		irodsFileHandle: handle,
		entry:           entry,
		offset:          offset,
		openMode:        types.FileOpenMode(mode),
	}

	fs.fileHandleMap.Add(fileHandle)
	return fileHandle, nil
}

// CreateFile opens a new file for write
func (fs *FileSystem) CreateFile(irodsPath string, resource string, mode string) (*FileHandle, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	// create
	keywords := map[common.KeyWord]string{}
	handle, err := irods_fs.CreateDataObject(conn, irodsCorrectPath, resource, mode, true, keywords)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	// close - this is required to let other processes see the file existence
	err = irods_fs.CloseDataObject(conn, handle)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	entry, err := fs.getDataObjectWithConnectionNoCache(conn, irodsCorrectPath)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	// re-open
	handle, offset, err := irods_fs.OpenDataObject(conn, irodsCorrectPath, resource, mode, keywords)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	// do not return connection here
	fileHandle := &FileHandle{
		id:              xid.New().String(),
		filesystem:      fs,
		connection:      conn,
		irodsFileHandle: handle,
		entry:           entry,
		offset:          offset,
		openMode:        types.FileOpenMode(mode),
	}

	fs.fileHandleMap.Add(fileHandle)
	fs.invalidateCacheForFileCreate(irodsCorrectPath)
	fs.cachePropagation.PropagateFileCreate(irodsCorrectPath)

	return fileHandle, nil
}

// getCollectionNoCache returns collection entry
func (fs *FileSystem) getCollectionNoCache(irodsPath string) (*Entry, error) {
	// retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	collection, err := irods_fs.GetCollection(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	if collection.ID > 0 {
		entry := NewEntryFromCollection(collection)

		// cache it
		fs.cache.RemoveNegativeEntryCache(irodsPath)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, xerrors.Errorf("failed to find the collection for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
}

// getCollection returns collection entry
func (fs *FileSystem) getCollection(irodsPath string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(irodsPath) {
		return nil, xerrors.Errorf("failed to find the collection for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(irodsPath)
	if cachedEntry != nil && cachedEntry.Type == DirectoryEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getCollectionNoCache(irodsPath)
}

// listEntries lists entries in a collection
func (fs *FileSystem) listEntries(collPath string) ([]*Entry, error) {
	// check cache first
	cachedEntries := []*Entry{}
	useCached := false

	cachedDirEntryPaths := fs.cache.GetDirCache(collPath)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedEntry := fs.cache.GetEntryCache(cachedDirEntryPath)
			if cachedEntry != nil {
				cachedEntries = append(cachedEntries, cachedEntry)
			} else {
				useCached = false
				break
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
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	collections, err := irods_fs.ListSubCollections(conn, collPath)
	if err != nil {
		return nil, err
	}

	entries := []*Entry{}

	for _, coll := range collections {
		entry := NewEntryFromCollection(coll)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	dataobjects, err := irods_fs.ListDataObjects(conn, collPath)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		entry := NewEntryFromDataObject(dataobject)
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
	fs.cache.AddDirCache(collPath, dirEntryPaths)

	return entries, nil
}

// getDataObjectWithConnectionNoCache returns an entry for data object
func (fs *FileSystem) getDataObjectWithConnectionNoCache(conn *connection.IRODSConnection, irodsPath string) (*Entry, error) {
	dataobject, err := irods_fs.GetDataObjectMasterReplica(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	if dataobject.ID > 0 {
		entry := NewEntryFromDataObject(dataobject)

		// cache it
		fs.cache.RemoveNegativeEntryCache(irodsPath)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, xerrors.Errorf("failed to find the data object for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
}

// getDataObjectWithConnection returns an entry for data object
func (fs *FileSystem) getDataObjectWithConnection(conn *connection.IRODSConnection, irodsPath string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(irodsPath) {
		return nil, xerrors.Errorf("failed to find the data object for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(irodsPath)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getDataObjectWithConnectionNoCache(conn, irodsPath)
}

// getDataObjectNoCache returns an entry for data object
func (fs *FileSystem) getDataObjectNoCache(irodsPath string) (*Entry, error) {
	// retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	dataobject, err := irods_fs.GetDataObjectMasterReplica(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	if dataobject.ID > 0 {
		entry := NewEntryFromDataObject(dataobject)

		// cache it
		fs.cache.RemoveNegativeEntryCache(irodsPath)
		fs.cache.AddEntryCache(entry)
		return entry, nil
	}

	return nil, xerrors.Errorf("failed to find the data object for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
}

// getDataObject returns an entry for data object
func (fs *FileSystem) getDataObject(irodsPath string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(irodsPath) {
		return nil, xerrors.Errorf("failed to find the data object for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(irodsPath)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getDataObjectNoCache(irodsPath)
}
