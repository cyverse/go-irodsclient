package types

import (
	"fmt"
)

// IRODSFileHandle contains file handle
type IRODSFileHandle struct {
	FileDescriptor int
	// Path has an absolute path to the data object
	Path string
}

// ToString stringifies the object
func (handle *IRODSFileHandle) ToString() string {
	return fmt.Sprintf("<IRODSFileHandle %d %s>", handle.FileDescriptor, handle.Path)
}
