package fs

import (
	"fmt"
	"sync"

	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// FileHandle is a handle for a file opened
type FileHandle struct {
	id              string
	filesystem      *FileSystem
	connection      *connection.IRODSConnection
	irodsfilehandle *types.IRODSFileHandle
	entry           *Entry
	offset          int64
	openmode        types.FileOpenMode
	mutex           sync.Mutex
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
	return handle.openmode
}

// IsReadMode returns true if file is opened with read mode
func (handle *FileHandle) IsReadMode() bool {
	return handle.openmode.IsRead()
}

// IsReadOnlyMode returns true if file is opened with read only mode
func (handle *FileHandle) IsReadOnlyMode() bool {
	return handle.openmode.IsReadOnly()
}

// IsWriteMode returns true if file is opened with write mode
func (handle *FileHandle) IsWriteMode() bool {
	return handle.openmode.IsWrite()
}

// IsWriteOnlyMode returns true if file is opened with write only mode
func (handle *FileHandle) IsWriteOnlyMode() bool {
	return handle.openmode.IsWriteOnly()
}

// GetIRODSFileHandle returns iRODS File Handle file
func (handle *FileHandle) GetIRODSFileHandle() *types.IRODSFileHandle {
	return handle.irodsfilehandle
}

// GetEntry returns Entry info
func (handle *FileHandle) GetEntry() *Entry {
	return handle.entry
}

// Close closes the file
func (handle *FileHandle) Close() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	defer handle.filesystem.session.ReturnConnection(handle.connection)

	if handle.IsWriteMode() {
		handle.filesystem.invalidateCacheForFileUpdate(handle.entry.Path)
	}

	handle.filesystem.fileHandleMap.Remove(handle.id)
	return irods_fs.CloseDataObject(handle.connection, handle.irodsfilehandle)
}

// Close closes the file
func (handle *FileHandle) closeWithoutFSHandleManagement() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	defer handle.filesystem.session.ReturnConnection(handle.connection)

	if handle.IsWriteMode() {
		handle.filesystem.invalidateCacheForFileUpdate(handle.entry.Path)
	}

	return irods_fs.CloseDataObject(handle.connection, handle.irodsfilehandle)
}

// Seek moves file pointer
func (handle *FileHandle) Seek(offset int64, whence types.Whence) (int64, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsfilehandle, offset, whence)
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

	err := irods_fs.TruncateDataObjectHandle(handle.connection, handle.irodsfilehandle, size)
	if err != nil {
		return err
	}

	return nil
}

// Read reads the file
func (handle *FileHandle) Read(length int) ([]byte, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsReadMode() {
		return nil, fmt.Errorf("file is opened with %s mode", handle.openmode)
	}

	bytes, err := irods_fs.ReadDataObject(handle.connection, handle.irodsfilehandle, length)
	if err != nil {
		return nil, err
	}

	handle.offset += int64(len(bytes))
	return bytes, nil
}

// ReadAt reads data from given offset
func (handle *FileHandle) ReadAt(offset int64, length int) ([]byte, error) {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsReadMode() {
		return nil, fmt.Errorf("file is opened with %s mode", handle.openmode)
	}

	if handle.offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsfilehandle, offset, types.SeekSet)
		if err != nil {
			return nil, err
		}

		handle.offset = newOffset

		if newOffset != offset {
			return nil, fmt.Errorf("failed to seek to %d", offset)
		}
	}

	bytes, err := irods_fs.ReadDataObject(handle.connection, handle.irodsfilehandle, length)
	if err != nil {
		return nil, err
	}

	handle.offset += int64(len(bytes))
	return bytes, nil
}

// Write writes the file
func (handle *FileHandle) Write(data []byte) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsWriteMode() {
		return fmt.Errorf("file is opened with %s mode", handle.openmode)
	}

	err := irods_fs.WriteDataObject(handle.connection, handle.irodsfilehandle, data)
	if err != nil {
		return err
	}

	handle.offset += int64(len(data))

	// update
	if handle.entry.Size < handle.offset+int64(len(data)) {
		handle.entry.Size = handle.offset + int64(len(data))
	}

	return nil
}

// WriteAt writes the file to given offset
func (handle *FileHandle) WriteAt(offset int64, data []byte) error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	if !handle.IsWriteMode() {
		return fmt.Errorf("file is opened with %s mode", handle.openmode)
	}

	if handle.offset != offset {
		newOffset, err := irods_fs.SeekDataObject(handle.connection, handle.irodsfilehandle, offset, types.SeekSet)
		if err != nil {
			return err
		}

		handle.offset = newOffset

		if newOffset != offset {
			return fmt.Errorf("failed to seek to %d", offset)
		}
	}

	err := irods_fs.WriteDataObject(handle.connection, handle.irodsfilehandle, data)
	if err != nil {
		return err
	}

	handle.offset += int64(len(data))

	// update
	if handle.entry.Size < handle.offset+int64(len(data)) {
		handle.entry.Size = handle.offset + int64(len(data))
	}

	return nil
}

// preprocessRename should be called before the file is renamed
func (handle *FileHandle) preprocessRename() error {
	// first, we need to close the file
	if handle.IsWriteMode() {
		handle.filesystem.invalidateCacheForFileUpdate(handle.entry.Path)
	}

	err := irods_fs.CloseDataObject(handle.connection, handle.irodsfilehandle)
	if err != nil {
		return err
	}

	return nil
}

// postprocessRename should be called after the file is renamed
func (handle *FileHandle) postprocessRename(newPath string, newEntry *Entry) error {
	// apply path change
	newOpenMode := types.FileOpenModeReadWrite
	switch handle.openmode {
	case types.FileOpenModeReadOnly:
		newOpenMode = handle.openmode
	case types.FileOpenModeReadWrite:
		newOpenMode = handle.openmode
	case types.FileOpenModeWriteOnly:
		newOpenMode = handle.openmode
	case types.FileOpenModeWriteTruncate:
		newOpenMode = types.FileOpenModeWriteOnly
	case types.FileOpenModeAppend:
		newOpenMode = handle.openmode
	case types.FileOpenModeReadAppend:
		newOpenMode = handle.openmode
	}

	// reopen
	newHandle, offset, err := irods_fs.OpenDataObject(handle.connection, newPath, handle.irodsfilehandle.Resource, string(newOpenMode))
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
			return fmt.Errorf("failed to seek to %d", handle.offset)
		}
	}

	fileHandle := &FileHandle{
		id:              handle.id,
		filesystem:      handle.filesystem,
		connection:      handle.connection,
		irodsfilehandle: newHandle, // updated
		entry:           newEntry,  // updated
		offset:          handle.offset,
		openmode:        newOpenMode, // updated
	}

	handle.filesystem.fileHandleMap.Add(fileHandle)
	return nil
}

// ToString stringifies the object
func (handle *FileHandle) ToString() string {
	return fmt.Sprintf("<FileHandle %d %s %s %s>", handle.entry.ID, handle.entry.Type, handle.entry.Name, handle.openmode)
}
