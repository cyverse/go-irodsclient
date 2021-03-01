package types

import "fmt"

// IRODSMetaItemType describes a type to set metadata on
type IRODSMetaItemType string

const (
	IRODSDataObjectMetaItemType IRODSMetaItemType = "-d"
	IRODSCollectionMetaItemType IRODSMetaItemType = "-C"
	IRODSResourceMetaItemType   IRODSMetaItemType = "-R"
	IRODSUserMetaItemType       IRODSMetaItemType = "-u"
)

// GetIRODSMetaItemType gets the irods metadata item type from an object.
func GetIRODSMetaItemType(data interface{}) (IRODSMetaItemType, error) {
	switch data.(type) {
	case IRODSDataObject:
		return IRODSDataObjectMetaItemType, nil
	case IRODSCollection:
		return IRODSCollectionMetaItemType, nil
	case IRODSUser:
		return IRODSUserMetaItemType, nil
	default:
		return "", fmt.Errorf("data type is unknown for irods metadata")
	}
}

// IRODSMeta contains irods metadata
type IRODSMeta struct {
	AVUID int64 // is ignored on metadata operations (set, add, mod, rm)
	Name  string
	Value string
	Units string
}

// ToString stringifies the object
func (meta *IRODSMeta) ToString() string {
	return fmt.Sprintf("<IRODSMeta %d %s %s %s>", meta.AVUID, meta.Name, meta.Value, meta.Units)
}

// IRODSMetaCollection contains irods data object information
type IRODSMetaCollection struct {
	Path string
}

// ToString stringifies the object
func (meta *IRODSMetaCollection) ToString() string {
	return fmt.Sprintf("<IRODSMetaCollection %s>", meta.Path)
}
