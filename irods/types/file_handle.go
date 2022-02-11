package types

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSFileHandle contains file handle
type IRODSFileHandle struct {
	FileDescriptor int
	// Path has an absolute path to the data object
	Path     string
	OpenMode FileOpenMode
	Resource string
	Oper     common.OperationType
}

// ToString stringifies the object
func (handle *IRODSFileHandle) ToString() string {
	return fmt.Sprintf("<IRODSFileHandle %d %s %s %s %d>", handle.FileDescriptor, handle.Path, handle.OpenMode, handle.Resource, handle.Oper)
}
