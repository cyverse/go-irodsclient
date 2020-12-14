package types

import "fmt"

// IRODSDataObject contains irods data object information
type IRODSDataObject struct {
	ID string
	// Collection is a collection containing the object
	Collection IRODSCollection
	// Path has an absolute path to the data object
	Path string
	// Name has only the name part of the path
	Name string
	// Attributes has attributes
	Attributes string
	// Replicas has replication information
	Replicas IRODSReplica
	// Meta has internal information
	Meta IRODSMetaCollection
}

// ToString stringifies the object
func (obj *IRODSDataObject) ToString() string {
	return fmt.Sprintf("<IRODSDataObject %s %s>", obj.ID, obj.Name)
}
