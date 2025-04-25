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
	ID   int64         `json:"id"`
	Name string        `json:"name"`
	Zone string        `json:"zone"`
	Type IRODSUserType `json:"type"`
}

// IsGroup returns true if type is IRODSUserRodsGroup
func (user *IRODSUser) IsGroup() bool {
	return user.Type == IRODSUserRodsGroup
}

// IsUser returns true if type is IRODSUserRodsUser
func (user *IRODSUser) IsUser() bool {
	return user.Type == IRODSUserRodsUser
}

// IsAdminGroup returns true if type is IRODSUserGroupAdmin
func (user *IRODSUser) IsAdminGroup() bool {
	return user.Type == IRODSUserGroupAdmin
}

// IsAdminUser returns true if type is IRODSUserRodsAdmin
func (user *IRODSUser) IsAdminUser() bool {
	return user.Type == IRODSUserRodsAdmin
}

// ToString stringifies the object
func (user *IRODSUser) ToString() string {
	return fmt.Sprintf("<IRODSUser %d %s %s %s>", user.ID, user.Name, user.Zone, string(user.Type))
}
