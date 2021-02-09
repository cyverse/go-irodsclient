package types

import (
	"fmt"
	"time"
)

// IRODSReplica contains irods data object replication information
type IRODSReplica struct {
	Number int64

	// Owner has the owner's name
	Owner string

	// CheckSum has the checksum of the file
	CheckSum     string
	Status       string
	ResourceName string

	// Path has an absolute path to the data object
	Path              string
	ResourceHierarchy string

	// CreateTime has creation time
	CreateTime time.Time
	// ModifyTime has last modified time
	ModifyTime time.Time
}

// ToString stringifies the object
func (obj *IRODSReplica) ToString() string {
	return fmt.Sprintf("<IRODSReplica %d %s %s %s %s %s>", obj.Number, obj.CheckSum, obj.Status, obj.ResourceName, obj.CreateTime, obj.ModifyTime)
}
