package types

import (
	"fmt"
)

// IRODSFileLockHandle contains file lock handle
type IRODSFileLockHandle struct {
	FileDescriptor int
	// Path has an absolute path to the data object
	Path     string
	OpenMode FileOpenMode
	Type     DataObjectLockType
	Command  DataObjectLockCommand
}

// ToString stringifies the object
func (handle *IRODSFileLockHandle) ToString() string {
	return fmt.Sprintf("<IRODSFileLockHandle %d %s %s %s %s>", handle.FileDescriptor, handle.Path, handle.OpenMode, handle.Type, handle.Command)
}
