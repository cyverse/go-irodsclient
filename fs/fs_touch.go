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

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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
		handle, offset, err := irods_fs.OpenDataObject(conn, irodsPath, resource, "a", keywords)
		if err != nil {
			return err
		}

		if entry.Size != offset {
			_, err := irods_fs.SeekDataObject(conn, handle, entry.Size, types.SeekSet)
			if err != nil {
				return err
			}
		}

		// write dummy
		dummy := "xxxdummyxxx"
		err = irods_fs.WriteDataObject(conn, handle, []byte(dummy))
		if err != nil {
			return err
		}

		// close
		err = irods_fs.CloseDataObject(conn, handle)
		if err != nil {
			return err
		}

		// truncate dummy part
		err = irods_fs.TruncateDataObject(conn, irodsPath, entry.Size)
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
