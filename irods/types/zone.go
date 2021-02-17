package types

import "fmt"

// IRODSZone contains irods zone information
type IRODSZone struct {
	ID   string
	Name string
	Type string
}

// ToString stringifies the object
func (zone *IRODSZone) ToString() string {
	return fmt.Sprintf("<IRODSZone %s %s %s>", zone.ID, zone.Name, zone.Type)
}
