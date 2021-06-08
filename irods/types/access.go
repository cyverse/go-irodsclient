package types

import "fmt"

type IRODSAccessLevelType string

const (
	IRODSAccessLevelOwner IRODSAccessLevelType = "own"
	IRODSAccessLevelWrite IRODSAccessLevelType = "modify object"
	IRODSAccessLevelRead  IRODSAccessLevelType = "read object"
	IRODSAccessLevelNone  IRODSAccessLevelType = ""
)

// ChmodString returns the string for update access control messages.
func (accessType IRODSAccessLevelType) ChmodString() string {
	switch accessType {
	case IRODSAccessLevelRead:
		return "read"
	case IRODSAccessLevelWrite:
		return "write"
	case IRODSAccessLevelNone:
		return "null"
	default:
		return string(accessType)
	}
}

// IRODSAccess contains irods access information
type IRODSAccess struct {
	Path        string
	UserName    string
	UserZone    string
	UserType    IRODSUserType
	AccessLevel IRODSAccessLevelType
}

// ToString stringifies the object
func (access *IRODSAccess) ToString() string {
	return fmt.Sprintf("<IRODSAccess %s %s %s %s %s>", access.Path, access.UserName, access.UserZone, string(access.UserType), string(access.AccessLevel))
}
