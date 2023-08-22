package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
)

// WLockDataObject locks data object with write lock (exclusive)
func (fs *FileSystem) WLockDataObject(path string, wait bool) (*FileLockHandle, error) {
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

// RLockDataObject locks data object with read lock
func (fs *FileSystem) RLockDataObject(path string, wait bool) (*FileLockHandle, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	stat, err := fs.Stat(irodsPath)
	if err != nil {
		return nil, err
	}

	conn, err := fs.ioSession.AcquireConnection()
	if err != nil {
		return nil, err
	}

	lockType := types.DataObjectLockTypeRead
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
