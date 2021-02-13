package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	irods_fs "github.com/iychoi/go-irodsclient/pkg/irods/fs"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	Connection *connection.IRODSConnection // TODO: Change this to connection pool
}

// NewFileSystem creates a new FileSystem
func NewFileSystem(conn *connection.IRODSConnection) *FileSystem {
	return &FileSystem{
		Connection: conn,
	}
}

func (fs *FileSystem) getCollection(path string) (*types.IRODSCollection, error) {
	// TODO: Add cache here
	collection, err := irods_fs.GetCollection(fs.Connection, path)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (fs *FileSystem) getDataObject(path string) (*types.IRODSDataObject, error) {
	// TODO: Add cache here
	collection, err := fs.getCollection(util.GetIRODSPathDirname(path))
	if err != nil {
		return nil, err
	}

	dataobject, err := irods_fs.GetDataObjectMasterReplica(fs.Connection, collection, util.GetIRODSPathFileName(path))
	if err != nil {
		return nil, err
	}
	return dataobject, nil
}

// Stat returns file status
func (fs *FileSystem) Stat(path string) (*FSEntry, error) {
	dirStat, err := fs.StatDir(path)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// fall
		} else {
			return nil, err
		}
	} else {
		return dirStat, nil
	}

	fileStat, err := fs.StatFile(path)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// fall
		} else {
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
	collection, err := fs.getCollection(path)
	if err != nil {
		return nil, err
	} else {
		if collection.ID > 0 {
			return &FSEntry{
				ID:         collection.ID,
				Type:       FSDirectoryEntry,
				Name:       collection.Name,
				Owner:      collection.Owner,
				Size:       0,
				CreateTime: collection.CreateTime,
				ModifyTime: collection.ModifyTime,
				CheckSum:   "",
				Internal:   collection,
			}, nil
		}
	}

	// not a collection, not a data object
	return nil, types.NewFileNotFoundErrorf("Could not find a directory")
}

// StatFile returns status of a file
func (fs *FileSystem) StatFile(path string) (*FSEntry, error) {
	dataobject, err := fs.getDataObject(path)
	if err != nil {
		return nil, err
	} else {
		if dataobject.ID > 0 {
			return &FSEntry{
				ID:         dataobject.ID,
				Type:       FSFileEntry,
				Name:       dataobject.Name,
				Owner:      dataobject.Replicas[0].Owner,
				Size:       dataobject.Size,
				CreateTime: dataobject.Replicas[0].CreateTime,
				ModifyTime: dataobject.Replicas[0].ModifyTime,
				CheckSum:   dataobject.Replicas[0].CheckSum,
				Internal:   dataobject,
			}, nil
		}
	}

	// not a collection, not a data object
	return nil, types.NewFileNotFoundErrorf("Could not find a data object")
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

// List lists all file system entries under the given path
func (fs *FileSystem) List(path string) ([]*FSEntry, error) {
	fsEntries := []*FSEntry{}

	collection, err := fs.getCollection(path)
	if err != nil {
		return nil, err
	}

	collections, err := irods_fs.ListSubCollections(fs.Connection, collection.Path)
	if err != nil {
		return nil, err
	}

	for _, collection := range collections {
		fsEntry := FSEntry{
			ID:         collection.ID,
			Type:       FSDirectoryEntry,
			Name:       collection.Name,
			Owner:      collection.Owner,
			Size:       0,
			CreateTime: collection.CreateTime,
			ModifyTime: collection.ModifyTime,
			CheckSum:   "",
			Internal:   collection,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	dataobjects, err := irods_fs.ListDataObjectsMasterReplica(fs.Connection, collection)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		replica := dataobject.Replicas[0]

		fsEntry := FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	return fsEntries, nil
}

// RemoveDir deletes a directory
func (fs *FileSystem) RemoveDir(path string, recurse bool, force bool) error {
	err := irods_fs.DeleteCollection(fs.Connection, path, recurse, force)
	if err != nil {
		return err
	}

	fs.clearCacheDir(path)

	return nil
}

// RemoveFile deletes a file
func (fs *FileSystem) RemoveFile(path string, force bool) error {
	err := irods_fs.DeleteDataObject(fs.Connection, path, force)
	if err != nil {
		return err
	}

	fs.clearCacheFile(path)

	return nil
}

// RenameDir renames a dir
func (fs *FileSystem) RenameDir(srcPath string, destPath string) error {
	destDirPath := destPath
	if fs.ExistsDir(destPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destDirPath = util.MakeIRODSPath(destPath, srcFileName)
	}

	return fs.RenameDirToDir(srcPath, destDirPath)
}

// RenameDirToDir renames a dir
func (fs *FileSystem) RenameDirToDir(srcPath string, destPath string) error {
	err := irods_fs.MoveCollection(fs.Connection, srcPath, destPath)
	if err != nil {
		return err
	}

	if util.GetIRODSPathDirname(srcPath) == util.GetIRODSPathDirname(destPath) {
		// from the same dir
		fs.clearCacheDir(util.GetIRODSPathDirname(srcPath))
	} else {
		fs.clearCacheDir(srcPath)
		fs.addCacheDir(destPath)
	}

	return nil
}

// RenameFile renames a file
func (fs *FileSystem) RenameFile(srcPath string, destPath string) error {
	destFilePath := destPath
	if fs.ExistsDir(destPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destFilePath = util.MakeIRODSPath(destPath, srcFileName)
	}

	return fs.RenameFileToFile(srcPath, destFilePath)
}

// RenameFileToFile renames a file
func (fs *FileSystem) RenameFileToFile(srcPath string, destPath string) error {
	err := irods_fs.MoveDataObject(fs.Connection, srcPath, destPath)
	if err != nil {
		return err
	}

	if util.GetIRODSPathDirname(srcPath) == util.GetIRODSPathDirname(destPath) {
		// from the same dir
		fs.clearCacheDir(util.GetIRODSPathDirname(srcPath))
	} else {
		fs.clearCacheFile(srcPath)
		fs.addCacheFile(destPath)
	}

	return nil
}

// MakeDir creates a directory
func (fs *FileSystem) MakeDir(path string, recurse bool) error {
	err := irods_fs.CreateCollection(fs.Connection, path, recurse)
	if err != nil {
		return err
	}

	fs.addCacheDir(path)

	return nil
}

// CopyFile copies a file
func (fs *FileSystem) CopyFile(srcPath string, destPath string) error {
	destFilePath := destPath
	if fs.ExistsDir(destPath) {
		// make full file name for dest
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destFilePath = util.MakeIRODSPath(destPath, srcFileName)
	}

	return fs.CopyFileToFile(srcPath, destFilePath)
}

// CopyFileToFile copies a file
func (fs *FileSystem) CopyFileToFile(srcPath string, destPath string) error {
	err := irods_fs.CopyDataObject(fs.Connection, srcPath, destPath)
	if err != nil {
		return err
	}

	fs.addCacheFile(destPath)
	return nil
}

// TruncateFile truncates a file
func (fs *FileSystem) TruncateFile(path string, size int64) error {
	err := irods_fs.TruncateDataObject(fs.Connection, path, size)
	if err != nil {
		return err
	}

	fs.addCacheFile(path)
	return nil
}

// ReplicateFile replicates a file
func (fs *FileSystem) ReplicateFile(path string, resource string, update bool) error {
	return irods_fs.ReplicateDataObject(fs.Connection, path, resource, update)
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, localPath string) error {
	localFilePath := localPath
	stat, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			localFilePath = localPath
		} else {
			return err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsPath)
			localFilePath = util.MakeIRODSPath(localPath, irodsFileName)
		} else {
			return fmt.Errorf("File %s already exists", localPath)
		}
	}

	return irods_fs.DownloadDataObject(fs.Connection, irodsPath, localFilePath)
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool) error {
	irodsFilePath := irodsPath

	stat, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return types.NewFileNotFoundError("Could not find the local file")
		} else {
			return err
		}
	} else {
		if stat.IsDir() {
			return types.NewFileNotFoundError("The local file is a directory")
		}
	}

	entry, err := fs.Stat(irodsPath)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// file not found
			// skip
		} else {
			return err
		}
	} else {
		switch entry.Type {
		case FSFileEntry:
			// do nothing
		case FSDirectoryEntry:
			localFileName := util.GetIRODSPathFileName(localPath)
			irodsFilePath = util.MakeIRODSPath(irodsPath, localFileName)
		default:
			return fmt.Errorf("Unknown entry type %s", entry.Type)
		}
	}

	err = irods_fs.UploadDataObject(fs.Connection, localPath, irodsFilePath, resource, replicate)
	if err != nil {
		return err
	}

	fs.addCacheFile(irodsFilePath)
	return nil
}

// OpenFile opens an existing file for read/write
func (fs *FileSystem) OpenFile(path string, resource string, mode string) (*FileHandle, error) {
	handle, offset, err := irods_fs.OpenDataObject(fs.Connection, path, resource, mode)
	if err != nil {
		return nil, err
	}

	var entry *FSEntry = nil
	if types.FileOpenMode(mode) == types.FileOpenModeReadOnly ||
		types.FileOpenMode(mode) == types.FileOpenModeReadWrite ||
		types.FileOpenMode(mode) == types.FileOpenModeAppend ||
		types.FileOpenMode(mode) == types.FileOpenModeReadAppend {
		// file may exists
		entryExisting, err := fs.StatFile(path)
		if err == nil {
			entry = entryExisting
		}
	}

	if entry == nil {
		// create a new
		entry = &FSEntry{
			ID:         0,
			Type:       FSFileEntry,
			Name:       util.GetIRODSPathFileName(path),
			Owner:      fs.Connection.Account.ClientUser,
			Size:       0,
			CreateTime: time.Now(),
			ModifyTime: time.Now(),
			CheckSum:   "",
			Internal:   nil,
		}
	}

	return &FileHandle{
		FileSystem:  fs,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      offset,
		OpenMode:    types.FileOpenMode(mode),
	}, nil
}

// CreateFile opens a new file for write
func (fs *FileSystem) CreateFile(path string, resource string) (*FileHandle, error) {
	handle, err := irods_fs.CreateDataObject(fs.Connection, path, resource, true)
	if err != nil {
		return nil, err
	}

	entry := &FSEntry{
		ID:         0,
		Type:       FSFileEntry,
		Name:       util.GetIRODSPathFileName(path),
		Owner:      fs.Connection.Account.ClientUser,
		Size:       0,
		CreateTime: time.Now(),
		ModifyTime: time.Now(),
		CheckSum:   "",
		Internal:   nil,
	}

	return &FileHandle{
		FileSystem:  fs,
		IRODSHandle: handle,
		Entry:       entry,
		Offset:      0,
		OpenMode:    types.FileOpenModeWriteOnly,
	}, nil
}
