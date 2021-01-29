package types

import "fmt"

// IRODSCollection contains irods collection information
type IRODSCollection struct {
	ID int
	// Path has an absolute path to the collection
	Path string
	// Name has only the name part of the path
	Name string
}

// ToString stringifies the object
func (coll *IRODSCollection) ToString() string {
	return fmt.Sprintf("<IRODSCollection %d %s>", coll.ID, coll.Path)
}
