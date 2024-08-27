package fs

import (
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
			err = irods_fs.Touch(conn, irodsCorrectPath, resource, noCreate)
			if err != nil {
				return err
			}

			fs.invalidateCacheForFileCreate(irodsCorrectPath)
			return nil
		}
		return err
	}

	if entry.IsDir() {
		// dir
		err = irods_fs.Touch(conn, irodsCorrectPath, resource, noCreate)
		if err != nil {
			return err
		}

		fs.invalidateCacheForDirUpdate(irodsCorrectPath)
		return nil
	}

	// file
	err = irods_fs.Touch(conn, irodsCorrectPath, resource, noCreate)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileUpdate(irodsCorrectPath)
	return nil
}
