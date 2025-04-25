package types

import (
	"fmt"
	"time"
)

// IRODSReplica contains irods data object replication information
type IRODSReplica struct {
	Number int64 `json:"number"`

	// Owner has the owner's name
	Owner string `json:"owner"`

	Checksum     *IRODSChecksum `json:"checksum,omitempty"`
	Status       string         `json:"status"`
	ResourceName string         `json:"resource_name"`

	// Path has an absolute path to the data object
	Path              string `json:"path"`
	ResourceHierarchy string `json:"resource_hierarchy"`

	// CreateTime has creation time
	CreateTime time.Time `json:"create_time"`
	// ModifyTime has last modified time
	ModifyTime time.Time `json:"modify_time"`
}

// ToString stringifies the object
func (obj *IRODSReplica) ToString() string {
	return fmt.Sprintf("<IRODSReplica %d %s %s %s %s>", obj.Number, obj.Status, obj.ResourceName, obj.CreateTime, obj.ModifyTime)
}
