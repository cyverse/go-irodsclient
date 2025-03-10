package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// SearchByMeta searches all file system entries with given metadata
func (fs *FileSystem) SearchByMeta(metaname string, metavalue string) ([]*Entry, error) {
	return fs.searchEntriesByMeta(metaname, metavalue)
}

// ListMetadata lists metadata for the given path
func (fs *FileSystem) ListMetadata(irodsPath string) ([]*types.IRODSMeta, error) {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	// check cache first
	cachedEntry := fs.cache.GetMetadataCache(irodsCorrectPath)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	var metadataobjects []*types.IRODSMeta

	if fs.ExistsDir(irodsCorrectPath) {
		metadataobjects, err = irods_fs.ListCollectionMeta(conn, irodsCorrectPath)
		if err != nil {
			return nil, err
		}
	} else {
		metadataobjects, err = irods_fs.ListDataObjectMeta(conn, irodsCorrectPath)
		if err != nil {
			return nil, err
		}
	}

	// cache it
	fs.cache.AddMetadataCache(irodsCorrectPath, metadataobjects)

	return metadataobjects, nil
}

// AddMetadata adds a metadata for the path
func (fs *FileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	if fs.ExistsDir(irodsCorrectPath) {
		err = irods_fs.AddCollectionMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	} else {
		err = irods_fs.AddDataObjectMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	}

	fs.cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// DeleteMetadata deletes a metadata for the path
func (fs *FileSystem) DeleteMetadata(irodsPath string, avuID int64) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	metadata := &types.IRODSMeta{
		AVUID: avuID,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	if fs.ExistsDir(irodsCorrectPath) {
		err = irods_fs.DeleteCollectionMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	} else {
		err = irods_fs.DeleteDataObjectMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	}

	fs.cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// DeleteMetadataByName deletes a metadata for the path by name
func (fs *FileSystem) DeleteMetadataByName(irodsPath string, attName string) error {
	irodsCorrectPath := util.GetCorrectIRODSPath(irodsPath)

	metadata := &types.IRODSMeta{
		AVUID: 0,
		Name:  attName,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	if fs.ExistsDir(irodsCorrectPath) {
		err = irods_fs.DeleteCollectionMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	} else {
		err = irods_fs.DeleteDataObjectMeta(conn, irodsCorrectPath, metadata)
		if err != nil {
			return err
		}
	}

	fs.cache.RemoveMetadataCache(irodsCorrectPath)
	return nil
}

// AddUserMetadata adds a user metadata
func (fs *FileSystem) AddUserMetadata(username string, zoneName string, attName string, attValue string, attUnits string) error {
	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddUserMeta(conn, username, zoneName, metadata)
	if err != nil {
		return err
	}

	return nil
}

// DeleteUserMetadata deletes a user metadata
func (fs *FileSystem) DeleteUserMetadata(username string, zoneName string, avuID int64) error {
	metadata := &types.IRODSMeta{
		AVUID: avuID,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteUserMeta(conn, username, zoneName, metadata)
	if err != nil {
		return err
	}

	return nil
}

// DeleteUserMetadataByName deletes a user metadata by name
func (fs *FileSystem) DeleteUserMetadataByName(username string, zoneName string, attName string) error {
	metadata := &types.IRODSMeta{
		AVUID: 0,
		Name:  attName,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteUserMeta(conn, username, zoneName, metadata)
	if err != nil {
		return err
	}

	return nil
}

// ListUserMetadata lists all user metadata
func (fs *FileSystem) ListUserMetadata(username string, zoneName string) ([]*types.IRODSMeta, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	metadataobjects, err := irods_fs.ListUserMeta(conn, username, zoneName)
	if err != nil {
		return nil, err
	}

	return metadataobjects, nil
}

// AddResourceMetadata adds a resource metadata
func (fs *FileSystem) AddResourceMetadata(resource string, attName, attValue, attUnits string) error {
	metadata := &types.IRODSMeta{
		Name:  attName,
		Value: attValue,
		Units: attUnits,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddResourceMeta(conn, resource, metadata)
	if err != nil {
		return err
	}

	return nil
}

// DeleteResourceMetadata deletes a resource metadata
func (fs *FileSystem) DeleteResourceMetadata(resource string, avuID int64) error {
	metadata := &types.IRODSMeta{
		AVUID: avuID,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteResourceMeta(conn, resource, metadata)
	if err != nil {
		return err
	}

	return nil
}

// DeleteResourceMetadataByName deletes a resource metadata by name
func (fs *FileSystem) DeleteResourceMetadataByName(resource string, attName string) error {
	metadata := &types.IRODSMeta{
		AVUID: 0,
		Name:  attName,
	}

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteResourceMeta(conn, resource, metadata)
	if err != nil {
		return err
	}

	return nil
}

// ListResourceMetadata lists all resource metadata
func (fs *FileSystem) ListResourceMetadata(resource string) ([]*types.IRODSMeta, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	metadataobjects, err := irods_fs.ListResourceMeta(conn, resource)
	if err != nil {
		return nil, err
	}

	return metadataobjects, nil
}

// searchEntriesByMeta searches entries by meta
func (fs *FileSystem) searchEntriesByMeta(metaName string, metaValue string) ([]*Entry, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	collections, err := irods_fs.SearchCollectionsByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	entries := []*Entry{}

	for _, coll := range collections {
		entry := fs.getEntryFromCollection(coll)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	dataobjects, err := irods_fs.SearchDataObjectsMasterReplicaByMeta(conn, metaName, metaValue)
	if err != nil {
		return nil, err
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		entry := fs.getEntryFromDataObject(dataobject)
		entries = append(entries, entry)

		// cache it
		fs.cache.RemoveNegativeEntryCache(entry.Path)
		fs.cache.AddEntryCache(entry)
	}

	return entries, nil
}
