package types

import "fmt"

// IRODSUserType is a type of iRODS User
type IRODSUserType string

const (
	// IRODSUserRodsGroup is for a group
	IRODSUserRodsGroup IRODSUserType = "rodsgroup"
	// IRODSUserRodsUser is for a user
	IRODSUserRodsUser IRODSUserType = "rodsuser"
	// IRODSUserRodsAdmin is for an admin user
	IRODSUserRodsAdmin IRODSUserType = "rodsadmin"
	// IRODSUserGroupAdmin is for an admin group
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
