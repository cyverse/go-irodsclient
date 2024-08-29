package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// ExtractStructFile extracts a struct file
func (fs *FileSystem) ExtractStructFile(path string, targetCollection string, resource string, dataType types.DataType, force bool, bulkReg bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)
	targetIrodsPath := util.GetCorrectIRODSPath(targetCollection)

	// we create a new connection for extraction because iRODS has a bug that does not clear file descriptors, causing SYS_OUT_OF_FILE_DESC error.
	// create a new unmanaged connection and throw out after use.
	conn, err := fs.metadataSession.AcquireUnmanagedConnection()
	if err != nil {
		return err
	}

	// discard the connection after use to avoid file descriptor error.
	defer fs.metadataSession.DiscardConnection(conn)

	err = irods_fs.ExtractStructFile(conn, irodsPath, targetIrodsPath, resource, dataType, force, bulkReg)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirExtract(targetIrodsPath)
	fs.cachePropagation.PropagateDirExtract(targetIrodsPath)

	return nil
}
