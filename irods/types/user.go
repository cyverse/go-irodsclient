package types

import "fmt"

type IRODSUserType string

const (
	IRODSUserRodsGroup  IRODSUserType = "rodsgroup"
	IRODSUserRodsUser   IRODSUserType = "rodsuser"
	IRODSUserRodsAdmin  IRODSUserType = "rodsadmin"
	IRODSUserGroupAdmin IRODSUserType = "groupadmin"
)

// IRODSUser contains irods user information
type IRODSUser struct {
	ID   int64
	Name string
	Zone string
	Type IRODSUserType
}

// ToString stringifies the object
func (user *IRODSUser) ToString() string {
	return fmt.Sprintf("<IRODSUser %d %s %s %s>", user.ID, user.Name, user.Zone, string(user.Type))
}
