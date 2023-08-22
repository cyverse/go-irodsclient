package fs

import (
	"fmt"
	"sync"

	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// FileLockHandle is a handle for a file lock opened
type FileLockHandle struct {
	id                  string
	filesystem          *FileSystem
	connection          *connection.IRODSConnection
	irodsfilelockhandle *types.IRODSFileLockHandle
	entry               *Entry
	lockType            types.DataObjectLockType
	lockCommand         types.DataObjectLockCommand
	mutex               sync.Mutex
}

// GetID returns ID
func (handle *FileLockHandle) GetID() string {
	return handle.id
}

// IsReadOnlyMode returns true if file lock is opened with read only mode
func (handle *FileLockHandle) IsReadOnlyMode() bool {
	return handle.lockType == types.DataObjectLockTypeRead
}

// IsWriteOnlyMode returns true if file lock is opened with write only mode
func (handle *FileLockHandle) IsWriteMode() bool {
	return handle.lockType == types.DataObjectLockTypeWrite
}

// GetLockType returns file lock type
func (handle *FileLockHandle) GetLockType() types.DataObjectLockType {
	return handle.lockType
}

// GetLockCommand returns file lock command
func (handle *FileLockHandle) GetLockCommand() types.DataObjectLockCommand {
	return handle.lockCommand
}

// GetIRODSFileLockHandle returns iRODS File Lock Handle
func (handle *FileLockHandle) GetIRODSFileLockHandle() *types.IRODSFileLockHandle {
	return handle.irodsfilelockhandle
}

// GetEntry returns Entry info
func (handle *FileLockHandle) GetEntry() *Entry {
	return handle.entry
}

// Close closes the file lock
func (handle *FileLockHandle) Unlock() error {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()

	defer handle.filesystem.ioSession.ReturnConnection(handle.connection)

	err := irods_fs.UnlockDataObject(handle.connection, handle.irodsfilelockhandle)
	handle.filesystem.fileLockHandleMap.Remove(handle.id)

	return err
}

// ToString stringifies the object
func (handle *FileLockHandle) ToString() string {
	return fmt.Sprintf("<FileLockHandle %d %s %s %s>", handle.irodsfilelockhandle.FileDescriptor, handle.irodsfilelockhandle.Path, handle.lockType, handle.lockCommand)
}
