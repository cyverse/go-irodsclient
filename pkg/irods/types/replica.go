package types

import "fmt"

// IRODSReplica contains irods data object replication information
type IRODSReplica struct {
	Number       int
	Status       string
	ResourceName string
	// Path has an absolute path to the data object
	Path              string
	ResourceHierarchy string
}

// ToString stringifies the object
func (obj *IRODSReplica) ToString() string {
	return fmt.Sprintf("<IRODSReplica %s>", obj.ResourceName)
}
