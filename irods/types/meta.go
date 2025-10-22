package types

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
)

// IRODSMetaItemType describes a type to set metadata on
type IRODSMetaItemType string

const (
	// IRODSDataObjectMetaItemType is a type for data object meta
	IRODSDataObjectMetaItemType IRODSMetaItemType = "-d"
	// IRODSCollectionMetaItemType is a type for collection meta
	IRODSCollectionMetaItemType IRODSMetaItemType = "-C"
	// IRODSResourceMetaItemType is a type for resource meta
	IRODSResourceMetaItemType IRODSMetaItemType = "-R"
	// IRODSUserMetaItemType is a type for user meta
	IRODSUserMetaItemType IRODSMetaItemType = "-u"
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
		return "", errors.Errorf("unknown irods metadata item type")
	}
}

// IRODSMeta contains irods metadata
type IRODSMeta struct {
	AVUID int64  `json:"avu_id"` // is ignored on metadata operations (set, add, mod, rm)
	Name  string `json:"name"`
	Value string `json:"value"`
	Units string `json:"units"`
	// CreateTime has creation time
	CreateTime time.Time `json:"create_time"`
	// ModifyTime has last modified time
	ModifyTime time.Time `json:"modify_time"`
}

// ToString stringifies the object
func (meta *IRODSMeta) ToString() string {
	return fmt.Sprintf("<IRODSMeta %d %s %s %s %s %s>", meta.AVUID, meta.Name, meta.Value, meta.Units, meta.CreateTime, meta.ModifyTime)
}
