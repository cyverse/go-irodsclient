package types

// ObjectType determines if the object is data object or collection in irods
type ObjectType string

const (
	// ObjectTypeDataObject is for DataObject
	ObjectTypeDataObject ObjectType = "data"
	// ObjectTypeCollection is for Collection
	ObjectTypeCollection ObjectType = "collection"
)
