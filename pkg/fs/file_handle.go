package fs

import (
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	irods_fs "github.com/iychoi/go-irodsclient/pkg/irods/fs"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// FileHandle ...
type FileHandle struct {
	FileSystem  *FileSystem
	Connection  *connection.IRODSConnection
	IRODSHandle *types.IRODSFileHandle
	Entry       *FSEntry
	Offset      int64
	OpenMode    types.FileOpenMode
}

// GetOffset returns current offset
func (handle *FileHandle) GetOffset() int64 {
	return handle.Offset
}

// Close closes the file
func (handle *FileHandle) Close() error {
	defer handle.FileSystem.Session.ReturnConnection(handle.Connection)

	if handle.OpenMode == types.FileOpenModeWriteOnly ||
		handle.OpenMode == types.FileOpenModeWriteTruncate ||
		handle.OpenMode == types.FileOpenModeAppend ||
		handle.OpenMode == types.FileOpenModeReadAppend {
		defer handle.FileSystem.invalidateCachePath(util.GetIRODSPathDirname(handle.Entry.Path))
	}

	return irods_fs.CloseDataObject(handle.Connection, handle.IRODSHandle)
}

// Seek moves file pointer
func (handle *FileHandle) Seek(offset int64, whence types.Whence) (int64, error) {
	newOffset, err := irods_fs.SeekDataObject(handle.Connection, handle.IRODSHandle, offset, whence)
	if err != nil {
		return newOffset, err
	}

	handle.Offset = newOffset
	return newOffset, nil
}

// Read reads the file
func (handle *FileHandle) Read(length int) ([]byte, error) {
	bytes, err := irods_fs.ReadDataObject(handle.Connection, handle.IRODSHandle, length)
	if err != nil {
		return nil, err
	}

	handle.Offset += int64(len(bytes))
	return bytes, nil
}

// Write writes the file
func (handle *FileHandle) Write(data []byte) error {
	err := irods_fs.WriteDataObject(handle.Connection, handle.IRODSHandle, data)
	if err != nil {
		return err
	}

	handle.Offset += int64(len(data))
	return nil
}

// ToString stringifies the object
func (handle *FileHandle) ToString() string {
	return fmt.Sprintf("<FileHandle %d %s %s %s>", handle.Entry.ID, handle.Entry.Type, handle.Entry.Name, handle.OpenMode)
}
