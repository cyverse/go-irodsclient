package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// ListProcesses lists all processes
func (fs *FileSystem) ListProcesses(address string, zone string) ([]*types.IRODSProcess, error) {
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	processes, err := irods_fs.StatProcess(conn, address, zone)
	if err != nil {
		return nil, err
	}

	return processes, nil
}

// ListGroups lists all groups
func (fs *FileSystem) ListAllProcesses() ([]*types.IRODSProcess, error) {
	return fs.ListProcesses("", "")
}
