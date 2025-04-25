package types

import (
	"fmt"
	"time"
)

// IRODSCollection contains irods collection information
type IRODSCollection struct {
	ID int64 `json:"id"`
	// Path has an absolute path to the collection
	Path string `json:"path"`
	// Name has only the name part of the path
	Name string `json:"name"`
	// Owner has the owner's name
	Owner string `json:"owner"`
	// CreateTime has creation time
	CreateTime time.Time `json:"create_time"`
	// ModifyTime has last modified time
	ModifyTime time.Time `json:"modify_time"`
}

// ToString stringifies the object
func (coll *IRODSCollection) ToString() string {
	return fmt.Sprintf("<IRODSCollection %d %s %s %s>", coll.ID, coll.Path, coll.CreateTime, coll.ModifyTime)
}
