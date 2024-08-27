package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// Touch creates an empty file or update timestamp
func (fs *FileSystem) Touch(irodsPath string, resource string, noCreate bool) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metaSession.ReturnConnection(conn)

	entry, err := fs.Stat(irodsCorrectPath)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// create
			err = fs.touchInternal(conn, entry, irodsCorrectPath, resource, noCreate)
			if err != nil {
				return err
			}

			fs.invalidateCacheForFileCreate(irodsCorrectPath)
			return nil
		}
		return err
	}

	err = fs.touchInternal(conn, entry, irodsCorrectPath, resource, noCreate)
	if err != nil {
		return err
	}

	if entry.IsDir() {
		fs.invalidateCacheForDirUpdate(irodsCorrectPath)
	} else {
		fs.invalidateCacheForFileUpdate(irodsCorrectPath)
	}

	return nil
}

func (fs *FileSystem) touchInternal(conn *connection.IRODSConnection, entry *Entry, irodsPath string, resource string, noCreate bool) error {
	err := irods_fs.Touch(conn, irodsPath, resource, noCreate)
	if err != nil {
		if !types.IsAPINotSupportedError(err) {
			return err
		}
	} else {
		return nil
	}

	// not supported
	if entry != nil {
		if entry.IsDir() {
			// do nothing
			// there's no way to update collection's timestamp
			return nil
		}

		// file
		// open
		keywords := map[common.KeyWord]string{}
		handle, _, err := irods_fs.OpenDataObject(conn, irodsPath, resource, "w", keywords)
		if err != nil {
			return err
		}

		// close
		err = irods_fs.CloseDataObject(conn, handle)
		if err != nil {
			return err
		}
	}

	// create
	keywords := map[common.KeyWord]string{}
	handle, err := irods_fs.CreateDataObject(conn, irodsPath, resource, "w", true, keywords)
	if err != nil {
		return err
	}

	// close
	err = irods_fs.CloseDataObject(conn, handle)
	if err != nil {
		return err
	}

	return nil
}
