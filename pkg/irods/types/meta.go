package types

import "fmt"

// IRODSMeta contains irods metadata
type IRODSMeta struct {
	AVUID string
	Name  string
	Value string
	Units string
}

// ToString stringifies the object
func (meta *IRODSMeta) ToString() string {
	return fmt.Sprintf("<IRODSMeta %s %s %s %s>", meta.AVUID, meta.Name, meta.Value, meta.Units)
}

// IRODSMetaCollection contains irods data object information
type IRODSMetaCollection struct {
	Path string
}

// ToString stringifies the object
func (meta *IRODSMetaCollection) ToString() string {
	return fmt.Sprintf("<IRODSMetaCollection %s>", meta.Path)
}
