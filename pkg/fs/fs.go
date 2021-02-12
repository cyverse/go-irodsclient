package fs

import (
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	irods_fs "github.com/iychoi/go-irodsclient/pkg/irods/fs"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// FileSystem provides a file-system like interface
type FileSystem struct {
	Connection *connection.IRODSConnection // TODO: Change this to connection pool
}

// NewFileSystem creates a new FileSystem
func NewFileSystem(conn *connection.IRODSConnection) *FileSystem {
	return &FileSystem{
		Connection: conn,
	}
}

func (fs *FileSystem) getCollection(path string) (*types.IRODSCollection, error) {
	// TODO: Add cache here
	collection, err := irods_fs.GetCollection(fs.Connection, path)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (fs *FileSystem) getDataObject(path string) (*types.IRODSDataObject, error) {
	// TODO: Add cache here
	collection, err := fs.getCollection(util.GetIRODSPathDirname(path))
	if err != nil {
		return nil, err
	}

	dataobject, err := irods_fs.GetDataObjectMasterReplica(fs.Connection, collection, util.GetIRODSPathFileName(path))
	if err != nil {
		return nil, err
	}
	return dataobject, nil
}

// Stat returns file status
func (fs *FileSystem) Stat(path string) (*FSEntry, error) {
	collection, err := fs.getCollection(path)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// file not found
			// not a collection
		} else {
			// error
			return nil, err
		}
	} else {
		if collection.ID > 0 {
			return &FSEntry{
				ID:         collection.ID,
				Type:       FSDirectoryEntry,
				Name:       collection.Name,
				Owner:      collection.Owner,
				Size:       0,
				CreateTime: collection.CreateTime,
				ModifyTime: collection.ModifyTime,
				CheckSum:   "",
				Internal:   collection,
			}, nil
		}
	}

	dataobject, err := fs.getDataObject(path)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// file not found
			// not a data object
		} else {
			// error
			return nil, err
		}
	} else {
		if dataobject.ID > 0 {
			return &FSEntry{
				ID:         dataobject.ID,
				Type:       FSFileEntry,
				Name:       dataobject.Name,
				Owner:      dataobject.Replicas[0].Owner,
				Size:       dataobject.Size,
				CreateTime: dataobject.Replicas[0].CreateTime,
				ModifyTime: dataobject.Replicas[0].ModifyTime,
				CheckSum:   dataobject.Replicas[0].CheckSum,
				Internal:   dataobject,
			}, nil
		}
	}

	// not a collection, not a data object
	return nil, types.NewFileNotFoundErrorf("Could not find a data object or a collection")
}

// Exists checks file/collection existance
func (fs *FileSystem) Exists(path string) bool {
	entry, err := fs.Stat(path)
	if err != nil {
		if _, ok := err.(*types.FileNotFoundError); ok {
			// file not found
			return false
		} else {
			// error
			return false
		}
	}
	if entry.ID > 0 {
		return true
	}
	return false
}

// List lists all file system entries under the given path
func (fs *FileSystem) List(path string) ([]*FSEntry, error) {
	fsEntries := []*FSEntry{}

	currentCollection, err := fs.getCollection(path)
	if err != nil {
		return nil, err
	}

	collections, err := irods_fs.ListSubCollections(fs.Connection, currentCollection.Path)
	if err != nil {
		return nil, fmt.Errorf("Could not list subcollections - %v", err)
	}

	for _, collection := range collections {
		fsEntry := FSEntry{
			ID:         collection.ID,
			Type:       FSDirectoryEntry,
			Name:       collection.Name,
			Owner:      collection.Owner,
			Size:       0,
			CreateTime: collection.CreateTime,
			ModifyTime: collection.ModifyTime,
			CheckSum:   "",
			Internal:   collection,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	dataobjects, err := irods_fs.ListDataObjectsMasterReplica(fs.Connection, currentCollection)
	if err != nil {
		return nil, fmt.Errorf("Could not list data objects - %v", err)
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		replica := dataobject.Replicas[0]

		fsEntry := FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Owner:      replica.Owner,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	return fsEntries, nil
}

// ListByCollection lists all file system entries under the given path
func (fs *FileSystem) ListByCollection(collection *types.IRODSCollection) ([]*FSEntry, error) {
	fsEntries := []*FSEntry{}

	collections, err := irods_fs.ListSubCollections(fs.Connection, collection.Path)
	if err != nil {
		return nil, fmt.Errorf("Could not list subcollections - %v", err)
	}

	for _, collection := range collections {
		fsEntry := FSEntry{
			ID:         collection.ID,
			Type:       FSDirectoryEntry,
			Name:       collection.Name,
			Size:       0,
			CreateTime: collection.CreateTime,
			ModifyTime: collection.ModifyTime,
			CheckSum:   "",
			Internal:   collection,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	dataobjects, err := irods_fs.ListDataObjects(fs.Connection, collection)
	if err != nil {
		return nil, fmt.Errorf("Could not list data objects - %v", err)
	}

	for _, dataobject := range dataobjects {
		if len(dataobject.Replicas) == 0 {
			continue
		}

		replica := dataobject.Replicas[0]

		fsEntry := FSEntry{
			ID:         dataobject.ID,
			Type:       FSFileEntry,
			Name:       dataobject.Name,
			Size:       dataobject.Size,
			CreateTime: replica.CreateTime,
			ModifyTime: replica.ModifyTime,
			CheckSum:   replica.CheckSum,
			Internal:   dataobject,
		}

		fsEntries = append(fsEntries, &fsEntry)
	}

	return fsEntries, nil
}
