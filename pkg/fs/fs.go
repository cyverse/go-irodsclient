package fs

import (
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/query"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
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
	collection, err := query.GetCollection(fs.Connection, path)
	if err != nil {
		return nil, fmt.Errorf("Could not get a collection - %v", err)
	}
	return collection, nil
}

// List lists all file system entries under the given path
func (fs *FileSystem) List(path string) ([]*FSEntry, error) {
	fsEntries := []*FSEntry{}

	currentCollection, err := fs.getCollection(path)
	if err != nil {
		return nil, fmt.Errorf("Could not get a collection - %v", err)
	}

	collections, err := query.ListSubCollections(fs.Connection, currentCollection.Path)
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

	dataobjects, err := query.ListDataObjectsMasterReplica(fs.Connection, currentCollection)
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

	collections, err := query.ListSubCollections(fs.Connection, collection.Path)
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

	dataobjects, err := query.ListDataObjects(fs.Connection, collection)
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
