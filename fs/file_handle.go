package fs

import (
	"fmt"
	"sync"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// FileHandle is a handle for a file opened
type FileHandle struct {
	id                  string
	filesystem          *FileSystem
	connection          *connection.IRODSConnection
	irodsFileHandle     *types.IRODSFileHandle
	irodsFileLockHandle *types.IRODSFileLockHandle
	entry               *Entry
	offset              int64
	openMode            types.FileOpenMode
	mutex               sync.Mutex
}

// GetID returns ID
func (handle *FileHandle) GetID() string {
	return handle.id
}

// Lock locks the handle
func (handle *FileHandle) Lock() {
	handle.mutex.Lock()
}

// Unlock unlocks the handle
func (handle *FileHandle) Unlock() {
	handle.mutex.Unlock()
}

// GetOffset returns current offset
func (handle *FileHandle) GetOffset() int64 {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	return handle.offset
}

// GetOpenMode returns file open mode
func (handle *FileHandle) GetOpenMode() types.FileOpenMode {
	return handle.openMode
}

// IsReadMode returns true if file is opened with read mode
func (handle *FileHandle) IsReadMode() bool {
	return handle.openMode.IsRead()
}

// IsReadOnlyMode returns true if file is opened with read only mode
func (handle *FileHandle) IsReadOnlyMode() bool {
	return handle.openMode.IsReadOnly()
}

// IsWriteMode returns true if file is opened with write mode
func (handle *FileHandle) IsWriteMode() bool {
	return handle.openMode.IsWrite()
}

// IsWriteOnlyMode returns true if file is opened with write only mode
func (handle *FileHandle) IsWriteOnlyMode() bool {
	return handle.openMode.IsWriteOnly()
}

// GetIRODSFileHandle returns iRODS File Handle
func (handle *FileHandle) GetIRODSFileHandle() *types.IRODSFileHandle {
	return handle.irodsFileHandle
}

// GetEntry returns Entry info
func (handle *FileHandle) GetEntry() *Entry {
	return handle.entry
}

// Close closes the file
func (handle *FileHandle) Close() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.irodsFileLockHandle != nil {
		// unlock if locked
		err := irods_fs.UnlockDataObject(handle.connection, handle.irodsFileLockHandle)
		if err != nil {
			return err
		}

		handle.irodsFileLockHandle = nil
	}

	defer handle.filesystem.ioSession.ReturnConnection(handle.connection) //nolint

	err := irods_fs.CloseDataObject(handle.connection, handle.irodsFileHandle)
	handle.filesystem.fileHandleMap.Remove(handle.id)

	if handle.IsWriteMode() {
		handle.filesystem.invalidateCacheForFileUpdate(handle.entry.Path)
		handle.filesystem.cachePropagation.PropagateFileUpdate(handle.entry.Path)
	}

	return err
}

// Seek moves file pointer
func (handle *FileHandle) Seek(offset int64, whence int) (int64, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsFileHandle, offset, types.Whence(whence))
	if err != nil {
		return newOffset, err
	}

	handle.offset = newOffset
	return newOffset, nil
}

// Truncate truncates the file
func (handle *FileHandle) Truncate(size int64) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	err := irods_fs.TruncateDataObjectHandle(handle.connection, handle.irodsFileHandle, size)
	if err != nil {
		return err
	}

	return nil
}

// Read reads the file, implements io.Reader.Read
func (handle *FileHandle) Read(buffer []byte) (int, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsReadMode() {
		return 0, xerrors.Errorf("file is opened with %q mode", handle.openMode)
	}

	readLen, err := irods_fs.ReadDataObject(handle.connection, handle.irodsFileHandle, buffer)
	if readLen > 0 {
		handle.offset += int64(readLen)
	}

	// it is possible to return readLen + EOF
	return readLen, err
}

// ReadAt reads data from given offset
func (handle *FileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsReadMode() {
		return 0, xerrors.Errorf("file is opened with %q mode", handle.openMode)
	}

	if handle.offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsFileHandle, offset, types.SeekSet)
		if err != nil {
			return 0, err
		}

		handle.offset = newOffset

		if newOffset != offset {
			return 0, xerrors.Errorf("failed to seek to %d", offset)
		}
	}

	readLen, err := irods_fs.ReadDataObject(handle.connection, handle.irodsFileHandle, buffer)
	if readLen > 0 {
		handle.offset += int64(readLen)
	}

	// it is possible to return readLen + EOF
	return readLen, err
}

// Write writes the file
func (handle *FileHandle) Write(data []byte) (int, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsWriteMode() {
		return 0, xerrors.Errorf("file is opened with %q mode", handle.openMode)
	}

	err := irods_fs.WriteDataObject(handle.connection, handle.irodsFileHandle, data)
	if err != nil {
		return 0, err
	}

	handle.offset += int64(len(data))

	// update
	if handle.entry.Size < handle.offset+int64(len(data)) {
		handle.entry.Size = handle.offset + int64(len(data))
	}

	return len(data), nil
}

// WriteAt writes the file to given offset
func (handle *FileHandle) WriteAt(data []byte, offset int64) (int, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsWriteMode() {
		return 0, xerrors.Errorf("file is opened with %q mode", handle.openMode)
	}

	if handle.offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsFileHandle, offset, types.SeekSet)
		if err != nil {
			return 0, err
		}

		handle.offset = newOffset

		if newOffset != offset {
			return 0, xerrors.Errorf("failed to seek to %d", offset)
		}
	}

	err := irods_fs.WriteDataObject(handle.connection, handle.irodsFileHandle, data)
	if err != nil {
		return 0, err
	}

	handle.offset += int64(len(data))

	// update
	if handle.entry.Size < handle.offset+int64(len(data)) {
		handle.entry.Size = handle.offset + int64(len(data))
	}

	return len(data), nil
}

// LockDataObject locks data object with write lock (exclusive)
func (handle *FileHandle) LockDataObject(wait bool) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	lockType := types.DataObjectLockTypeWrite
	lockCommand := types.DataObjectLockCommandSetLock
	if wait {
		lockCommand = types.DataObjectLockCommandSetLockWait
	}

	fileLockHandle, err := irods_fs.LockDataObject(handle.connection, handle.irodsFileHandle.Path, lockType, lockCommand)
	if err != nil {
		return err
	}

	handle.irodsFileLockHandle = fileLockHandle

	return nil
}

// RLockDataObject locks data object with read lock
func (handle *FileHandle) RLockDataObject(wait bool) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	lockType := types.DataObjectLockTypeRead
	lockCommand := types.DataObjectLockCommandSetLock
	if wait {
		lockCommand = types.DataObjectLockCommandSetLockWait
	}

	fileLockHandle, err := irods_fs.LockDataObject(handle.connection, handle.irodsFileHandle.Path, lockType, lockCommand)
	if err != nil {
		return err
	}

	handle.irodsFileLockHandle = fileLockHandle

	return nil
}

// UnlockDataObject unlocks data object
func (handle *FileHandle) UnlockDataObject() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if handle.irodsFileLockHandle != nil {
		err := irods_fs.UnlockDataObject(handle.connection, handle.irodsFileLockHandle)
		if err != nil {
			return err
		}

		handle.irodsFileLockHandle = nil
	}

	return nil
}

// preprocessRename should be called before the file is renamed
func (handle *FileHandle) preprocessRename() error {
	// first, we need to close the file
	err := irods_fs.CloseDataObject(handle.connection, handle.irodsFileHandle)

	if handle.IsWriteMode() {
		handle.filesystem.invalidateCacheForFileUpdate(handle.entry.Path)
		handle.filesystem.cachePropagation.PropagateFileUpdate(handle.entry.Path)
	}

	return err
}

// postprocessRename should be called after the file is renamed
func (handle *FileHandle) postprocessRename(newPath string, newEntry *Entry) error {
	// apply path change
	newOpenMode := types.FileOpenModeReadWrite
	switch handle.openMode {
	case types.FileOpenModeReadOnly:
		newOpenMode = handle.openMode
	case types.FileOpenModeReadWrite:
		newOpenMode = handle.openMode
	case types.FileOpenModeWriteOnly:
		newOpenMode = handle.openMode
	case types.FileOpenModeWriteTruncate:
		newOpenMode = types.FileOpenModeWriteOnly
	case types.FileOpenModeAppend:
		newOpenMode = handle.openMode
	case types.FileOpenModeReadAppend:
		newOpenMode = handle.openMode
	}

	// reopen
	keywords := map[common.KeyWord]string{}
	newHandle, offset, err := irods_fs.OpenDataObject(handle.connection, newPath, handle.irodsFileHandle.Resource, string(newOpenMode), keywords)
	if err != nil {
		return err
	}

	// seek
	if offset != handle.offset {
		newOffset, err := irods_fs.SeekDataObject(handle.connection, newHandle, handle.offset, types.SeekSet)
		if err != nil {
			return err
		}

		if handle.offset != newOffset {
			return xerrors.Errorf("failed to seek to %d", handle.offset)
		}
	}

	handle.irodsFileHandle = newHandle
	handle.entry = newEntry
	handle.openMode = newOpenMode
	return nil
}

// ToString stringifies the object
func (handle *FileHandle) ToString() string {
	return fmt.Sprintf("<FileHandle %d %q %q %q>", handle.entry.ID, handle.entry.Type, handle.entry.Name, handle.openMode)
}
