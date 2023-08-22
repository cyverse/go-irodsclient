package fs

import (
	"strings"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
)

// LockDataObject locks data object
func (fs *FileSystem) LockDataObject(path string, lockMode string, wait bool) (*FileLockHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	stat, err := fs.Stat(irodsPath)
	if err != nil {
		return nil, err
	}

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	lockType := types.DataObjectLockTypeWrite
	switch strings.ToLower(lockMode) {
	case "r", "ro", "readonly":
		lockType = types.DataObjectLockTypeRead
	case "w", "rw", "wo", "readwrite", "writeonly":
		lockType = types.DataObjectLockTypeWrite
	}

	lockCommand := types.DataObjectLockCommandSetLock
	if wait {
		lockCommand = types.DataObjectLockCommandSetLockWait
	}

	handle, err := irods_fs.LockDataObject(conn, irodsPath, lockType, lockCommand)
	if err != nil {
		fs.ioSession.ReturnConnection(conn)
		return nil, err
	}

	// do not return connection here
	fileLockHandle := &FileLockHandle{
		id:                  xid.New().String(),
		filesystem:          fs,
		connection:          conn,
		irodsfilelockhandle: handle,
		entry:               stat,
		lockType:            lockType,
		lockCommand:         lockCommand,
	}

	fs.fileLockHandleMap.Add(fileLockHandle)
	return fileLockHandle, nil
}
