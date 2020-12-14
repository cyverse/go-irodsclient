package types

import "fmt"

// IRODSAccess contains irods access information
type IRODSAccess struct {
	Name     string
	Path     string
	UserName string
	UserZone string
}

// ToString stringifies the object
func (access *IRODSAccess) ToString() string {
	return fmt.Sprintf("<IRODSAccess %s %s %s %s>", access.Name, access.Path, access.UserName, access.UserZone)
}
