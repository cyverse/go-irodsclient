package fs

import (
	"fmt"
	"sync"

	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// FileHandle ...
type FileHandle struct {
	ID          string
	FileSystem  *FileSystem
	Connection  *connection.IRODSConnection
	IRODSHandle *types.IRODSFileHandle
	Entry       *Entry
	Offset      int64
	OpenMode    types.FileOpenMode
	Mutex       sync.Mutex
}

// GetOffset returns current offset
func (handle *FileHandle) GetOffset() int64 {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	return handle.Offset
}

// IsReadMode returns true if file is opened with read mode
func (handle *FileHandle) IsReadMode() bool {
	return types.IsFileOpenFlagRead(handle.OpenMode)
}

// IsWriteMode returns true if file is opened with write mode
func (handle *FileHandle) IsWriteMode() bool {
	return types.IsFileOpenFlagWrite(handle.OpenMode)
}

// Close closes the file
func (handle *FileHandle) Close() error {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	defer handle.FileSystem.session.ReturnConnection(handle.Connection)

	if handle.IsWriteMode() {
		handle.FileSystem.invalidateCachePathRecursively(handle.Entry.Path)
	}

	handle.FileSystem.mutex.Lock()
	delete(handle.FileSystem.fileHandles, handle.ID)
	handle.FileSystem.mutex.Unlock()

	return irods_fs.CloseDataObject(handle.Connection, handle.IRODSHandle)
}

// Close closes the file
func (handle *FileHandle) closeWithoutFSHandleManagement() error {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	defer handle.FileSystem.session.ReturnConnection(handle.Connection)

	if handle.IsWriteMode() {
		handle.FileSystem.invalidateCachePathRecursively(handle.Entry.Path)
	}

	return irods_fs.CloseDataObject(handle.Connection, handle.IRODSHandle)
}

// Seek moves file pointer
func (handle *FileHandle) Seek(offset int64, whence types.Whence) (int64, error) {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	newOffset, err := irods_fs.SeekDataObject(handle.Connection, handle.IRODSHandle, offset, whence)
	if err != nil {
		return newOffset, err
	}

	handle.Offset = newOffset
	return newOffset, nil
}

// Read reads the file
func (handle *FileHandle) Read(length int) ([]byte, error) {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	if !handle.IsReadMode() {
		return nil, fmt.Errorf("file is opened with %s mode", handle.OpenMode)
	}

	bytes, err := irods_fs.ReadDataObject(handle.Connection, handle.IRODSHandle, length)
	if err != nil {
		return nil, err
	}

	handle.Offset += int64(len(bytes))
	return bytes, nil
}

// ReadAt reads data from given offset
func (handle *FileHandle) ReadAt(offset int64, length int) ([]byte, error) {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	if !handle.IsReadMode() {
		return nil, fmt.Errorf("file is opened with %s mode", handle.OpenMode)
	}

	if handle.Offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.Connection, handle.IRODSHandle, offset, types.SeekSet)
		if err != nil {
			return nil, err
		}

		handle.Offset = newOffset

		if newOffset != offset {
			return nil, fmt.Errorf("failed to seek to %d", offset)
		}
	}

	bytes, err := irods_fs.ReadDataObject(handle.Connection, handle.IRODSHandle, length)
	if err != nil {
		return nil, err
	}

	handle.Offset += int64(len(bytes))
	return bytes, nil
}

// Write writes the file
func (handle *FileHandle) Write(data []byte) error {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	if !handle.IsWriteMode() {
		return fmt.Errorf("file is opened with %s mode", handle.OpenMode)
	}

	err := irods_fs.WriteDataObject(handle.Connection, handle.IRODSHandle, data)
	if err != nil {
		return err
	}

	handle.Offset += int64(len(data))

	// update
	if handle.Entry.Size < handle.Offset+int64(len(data)) {
		handle.Entry.Size = handle.Offset + int64(len(data))
	}

	return nil
}

// WriteAt writes the file to given offset
func (handle *FileHandle) WriteAt(offset int64, data []byte) error {
	handle.Mutex.Lock()
	defer handle.Mutex.Unlock()

	if !handle.IsWriteMode() {
		return fmt.Errorf("file is opened with %s mode", handle.OpenMode)
	}

	if handle.Offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.Connection, handle.IRODSHandle, offset, types.SeekSet)
		if err != nil {
			return err
		}

		handle.Offset = newOffset

		if newOffset != offset {
			return fmt.Errorf("failed to seek to %d", offset)
		}
	}

	err := irods_fs.WriteDataObject(handle.Connection, handle.IRODSHandle, data)
	if err != nil {
		return err
	}

	handle.Offset += int64(len(data))

	// update
	if handle.Entry.Size < handle.Offset+int64(len(data)) {
		handle.Entry.Size = handle.Offset + int64(len(data))
	}

	return nil
}

// ToString stringifies the object
func (handle *FileHandle) ToString() string {
	return fmt.Sprintf("<FileHandle %d %s %s %s>", handle.Entry.ID, handle.Entry.Type, handle.Entry.Name, handle.OpenMode)
}
