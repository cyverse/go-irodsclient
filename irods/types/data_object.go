package types

import (
	"fmt"
)

// IRODSDataObject contains irods data object information
type IRODSDataObject struct {
	ID           int64 `json:"id"`
	CollectionID int64 `json:"collection_id"`
	// Path has an absolute path to the data object
	Path string `json:"path"`
	// Name has only the name part of the path
	Name string `json:"name"`
	// Size has the file size
	Size int64 `json:"size"`
	// DataType has the type of the file,
	DataType string `json:"data_type"`
	// Replicas has replication information
	Replicas []*IRODSReplica `json:"replicas,omitempty"`
}

// ToString stringifies the object
func (obj *IRODSDataObject) ToString() string {
	return fmt.Sprintf("<IRODSDataObject %d %s %d %s>", obj.ID, obj.Path, obj.Size, obj.DataType)
}
