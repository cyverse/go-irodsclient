package fs

import (
	"path"
	"sync"
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

// SupportParallelUpload returns if the server supports parallel upload
func (fs *FileSystem) SupportParallelUpload() bool {
	return fs.metadataSession.SupportParallelUpload()
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
func (fs *FileSystem) Stat(p string) (*Entry, error) {
	irodsPath := util.GetCorrectIRODSPath(p)

	// check if a negative cache for the given path exists
	if fs.cache.HasNegativeEntryCache(irodsPath) {
		// has a negative cache - fail fast
		return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
	}

	// check if a cached Entry for the given path exists
	cachedEntry := fs.cache.GetEntryCache(irodsPath)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// check if a cached dir Entry for the given path exists
	parentPath := path.Dir(irodsPath)
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
			return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
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
	return nil, xerrors.Errorf("failed to find the data object or the collection for path %q: %w", irodsPath, types.NewFileNotFoundError(irodsPath))
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

// RemoveDir deletes a directory
func (fs *FileSystem) RemoveDir(path string, recurse bool, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteCollection(conn, irodsPath, recurse, force)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			fs.invalidateCacheForFileRemove(irodsPath)
			fs.cachePropagation.PropagateFileRemove(irodsPath)
		}
		return err
	}

	fs.invalidateCacheForDirRemove(irodsPath, recurse)
	fs.cachePropagation.PropagateDirRemove(irodsPath)
	return nil
}

// RemoveFile deletes a file
func (fs *FileSystem) RemoveFile(path string, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	// if file handle is opened, wg
	wg := sync.WaitGroup{}
	wg.Add(1)

	eventHandlerID := fs.fileHandleMap.AddCloseEventHandler(irodsPath, func(path, id string, empty bool) {
		if empty {
			wg.Done()
		}
	})

	defer fs.fileHandleMap.RemoveCloseEventHandler(eventHandlerID)

	if util.WaitTimeout(&wg, time.Duration(fs.config.MetadataConnection.OperationTimeout)) {
		// timed out
		return xerrors.Errorf("failed to remove file, there are files still opened")
	}

	// wait done
	err = irods_fs.DeleteDataObject(conn, irodsPath, force)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			fs.invalidateCacheForFileRemove(irodsPath)
			fs.cachePropagation.PropagateFileRemove(irodsPath)
		}
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
func (fs *FileSystem) MakeDir(path string, recurse bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	dirEntry, err := fs.StatDir(path)
	if err == nil {
		if dirEntry.ID > 0 {
			// already exists
			if recurse {
				return nil
			}
			return types.NewFileAlreadyExistError(path)
		}
	}

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
func (fs *FileSystem) TruncateFile(path string, size int64) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	if size < 0 {
		size = 0
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ReplicateDataObject(conn, irodsPath, resource, update, false)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsPath)
	fs.cachePropagation.PropagateFileUpdate(irodsPath)
	return nil
}

// OpenFile opens an existing file for read/write
func (fs *FileSystem) OpenFile(path string, resource string, mode string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	keywords := map[common.KeyWord]string{}
	handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, mode, keywords)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
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
			ID:                0,
			Type:              FileEntry,
			Name:              util.GetIRODSPathFileName(irodsPath),
			Path:              irodsPath,
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
func (fs *FileSystem) CreateFile(path string, resource string, mode string) (*FileHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	// create
	keywords := map[common.KeyWord]string{}
	handle, err := irods_fs.CreateDataObject(conn, irodsPath, resource, mode, true, keywords)
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

	entry, err := fs.getDataObjectWithConnectionNoCache(conn, irodsPath)
	if err != nil {
		fs.ioSession.ReturnConnection(conn) //nolint
		return nil, err
	}

	// re-open
	handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, mode, keywords)
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
	fs.invalidateCacheForFileCreate(irodsPath)
	fs.cachePropagation.PropagateFileCreate(irodsPath)

	return fileHandle, nil
}

// getCollectionNoCache returns collection entry
func (fs *FileSystem) getCollectionNoCache(path string) (*Entry, error) {
	// retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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

	return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
}

// getCollection returns collection entry
func (fs *FileSystem) getCollection(path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
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
		ID:                collection.ID,
		Type:              DirectoryEntry,
		Name:              collection.Name,
		Path:              collection.Path,
		Owner:             collection.Owner,
		Size:              0,
		DataType:          "",
		CreateTime:        collection.CreateTime,
		ModifyTime:        collection.ModifyTime,
		CheckSumAlgorithm: types.ChecksumAlgorithmUnknown,
		CheckSum:          nil,
	}
}

func (fs *FileSystem) getEntryFromDataObject(dataobject *types.IRODSDataObject) *Entry {
	checksum := dataobject.Replicas[0].Checksum

	checksumAlgorithm := types.ChecksumAlgorithmUnknown
	var checksumString []byte

	if checksum != nil && len(checksum.Checksum) > 0 {
		checksumAlgorithm = checksum.Algorithm
		checksumString = checksum.Checksum
	}

	return &Entry{
		ID:                dataobject.ID,
		Type:              FileEntry,
		Name:              dataobject.Name,
		Path:              dataobject.Path,
		Owner:             dataobject.Replicas[0].Owner,
		Size:              dataobject.Size,
		DataType:          dataobject.DataType,
		CreateTime:        dataobject.Replicas[0].CreateTime,
		ModifyTime:        dataobject.Replicas[0].ModifyTime,
		CheckSumAlgorithm: checksumAlgorithm,
		CheckSum:          checksumString,
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
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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

	return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
}

// getDataObjectWithConnection returns an entry for data object
func (fs *FileSystem) getDataObjectWithConnection(conn *connection.IRODSConnection, path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
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
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	dataobject, err := irods_fs.GetDataObjectMasterReplicaWithoutCollection(conn, path)
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

	return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
}

// getDataObject returns an entry for data object
func (fs *FileSystem) getDataObject(path string) (*Entry, error) {
	if fs.cache.HasNegativeEntryCache(path) {
		return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
	}

	// check cache first
	cachedEntry := fs.cache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == FileEntry {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	return fs.getDataObjectNoCache(path)
}
