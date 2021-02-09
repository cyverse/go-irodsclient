package types

import (
	"fmt"
	"time"
)

// IRODSCollection contains irods collection information
type IRODSCollection struct {
	ID int64
	// Path has an absolute path to the collection
	Path string
	// Name has only the name part of the path
	Name string
	// Owner has the owner's name
	Owner string
	// CreateTime has creation time
	CreateTime time.Time
	// ModifyTime has last modified time
	ModifyTime time.Time
}

// ToString stringifies the object
func (coll *IRODSCollection) ToString() string {
	return fmt.Sprintf("<IRODSCollection %d %s %s %s>", coll.ID, coll.Path, coll.CreateTime, coll.ModifyTime)
}
