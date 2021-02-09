package types

import (
	"fmt"
)

// IRODSDataObject contains irods data object information
type IRODSDataObject struct {
	ID           int64
	CollectionID int64
	// Path has an absolute path to the data object
	Path string
	// Name has only the name part of the path
	Name string
	// Size has the file size
	Size int64
	// Attributes has attributes
	//Attributes string
	// Replicas has replication information
	Replicas []*IRODSReplica
}

// ToString stringifies the object
func (obj *IRODSDataObject) ToString() string {
	return fmt.Sprintf("<IRODSDataObject %d %s %d>", obj.ID, obj.Path, obj.Size)
}
