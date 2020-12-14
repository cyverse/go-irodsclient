package types

import "fmt"

// IRODSUser contains irods user information
type IRODSUser struct {
	ID   string
	Name string
	Type string
	Zone string
}

// ToString stringifies the object
func (user *IRODSUser) ToString() string {
	return fmt.Sprintf("<IRODSUser %s %s %s %s>", user.ID, user.Name, user.Type, user.Zone)
}

// IRODSUserGroup contains irods user group information
type IRODSUserGroup struct {
	ID   string
	Name string
}

// ToString stringifies the object
func (group *IRODSUserGroup) ToString() string {
	return fmt.Sprintf("<IRODSUserGroup %s %s>", group.ID, group.Name)
}
