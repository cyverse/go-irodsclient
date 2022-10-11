package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// ExtractStructFile extracts a struct file
func (fs *FileSystem) ExtractStructFile(path string, targetCollection string, resource string, dataType types.DataType, force bool) error {
	irodsPath := util.GetCorrectIRODSPath(path)
	targetIrodsPath := util.GetCorrectIRODSPath(targetCollection)

	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return err
	}

	// we do not return the connection for reuse. This does not clear file descriptors, causing SYS_OUT_OF_FILE_DESC error
	//defer fs.session.ReturnConnection(conn)
	defer fs.session.DiscardConnection(conn)

	err = irods_fs.ExtractStructFile(conn, irodsPath, targetIrodsPath, resource, dataType, force)
	if err != nil {
		return err
	}

	fs.invalidateCacheForDirCreate(targetIrodsPath)
	fs.cachePropagation.PropagateDirCreate(targetIrodsPath)

	return nil
}
