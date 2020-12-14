package types

import "fmt"

// IRODSCollection contains irods collection information
type IRODSCollection struct {
	ID string
	// Path has an absolute path to the collection
	Path string
	// Name has only the name part of the path
	Name string
	// Meta has internal information
	Meta IRODSMetaCollection
}

// ToString stringifies the object
func (coll *IRODSCollection) ToString() string {
	return fmt.Sprintf("<IRODSCollection %s %s>", coll.ID, coll.Name)
}
