package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// StatProcess stats processes
func (fs *FileSystem) StatProcess(address string, zoneName string) ([]*types.IRODSProcess, error) {
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	processes, err := irods_fs.StatProcess(conn, address, zoneName)
	if err != nil {
		return nil, err
	}

	return processes, err
}
