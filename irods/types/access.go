package types

import (
	"fmt"
	"strings"
)

// IRODSAccessLevelType is a type for access level
type IRODSAccessLevelType string

const (
	// IRODSAccessLevelNull is for null access
	IRODSAccessLevelNull IRODSAccessLevelType = "null"
	// IRODSAccessLevelExecute is for execute access
	IRODSAccessLevelExecute IRODSAccessLevelType = "execute"
	// IRODSAccessLevelReadAnnotation is for read annotation access
	IRODSAccessLevelReadAnnotation IRODSAccessLevelType = "read_annotation"
	// IRODSAccessLevelReadSystemMetadata is for read system metadata access
	IRODSAccessLevelReadSystemMetadata IRODSAccessLevelType = "read_system_metadata"
	// IRODSAccessLevelReadMetadata is for read metadata access
	IRODSAccessLevelReadMetadata IRODSAccessLevelType = "read_metadata"
	// IRODSAccessLevelReadObject is for read object access
	IRODSAccessLevelReadObject IRODSAccessLevelType = "read_object"
	// IRODSAccessLevelWriteAnnotation is for write annotation access
	IRODSAccessLevelWriteAnnotation IRODSAccessLevelType = "write_annotation"
	// IRODSAccessLevelCreateMetadata is for create metadata access
	IRODSAccessLevelCreateMetadata IRODSAccessLevelType = "create_metadata"
	// IRODSAccessLevelModifyMetadata is for modify metadata access
	IRODSAccessLevelModifyMetadata IRODSAccessLevelType = "modify_metadata"
	// IRODSAccessLevelDeleteMetadata is for delete metadata access
	IRODSAccessLevelDeleteMetadata IRODSAccessLevelType = "delete_metadata"
	// IRODSAccessLevelAdministerObject is for administer object access
	IRODSAccessLevelAdministerObject IRODSAccessLevelType = "administer_object"
	// IRODSAccessLevelCreateObject is for create object access
	IRODSAccessLevelCreateObject IRODSAccessLevelType = "create_object"
	// IRODSAccessLevelModifyObject is for modify object access
	IRODSAccessLevelModifyObject IRODSAccessLevelType = "modify_object"
	// IRODSAccessLevelDeleteObject is for delete object access
	IRODSAccessLevelDeleteObject IRODSAccessLevelType = "delete_object"
	// IRODSAccessLevelCreateToken is for create token access
	IRODSAccessLevelCreateToken IRODSAccessLevelType = "create_token"
	// IRODSAccessLevelDeleteToken is for delete token access
	IRODSAccessLevelDeleteToken IRODSAccessLevelType = "delete_token"
	// IRODSAccessLevelCurate is for curate access
	IRODSAccessLevelCurate IRODSAccessLevelType = "curate"
	// IRODSAccessLevelOwner is for owner access
	IRODSAccessLevelOwner IRODSAccessLevelType = "own"
)

func GetIRODSAccessLevelType(accessLevelType string) IRODSAccessLevelType {
	canonical := strings.ToLower(accessLevelType)
	canonical = strings.TrimSpace(canonical)
	canonical = strings.ReplaceAll(canonical, " ", "_")

	switch canonical {
	case string(IRODSAccessLevelExecute):
		return IRODSAccessLevelExecute
	case string(IRODSAccessLevelReadAnnotation):
		return IRODSAccessLevelReadAnnotation
	case string(IRODSAccessLevelReadSystemMetadata):
		return IRODSAccessLevelReadSystemMetadata
	case string(IRODSAccessLevelReadMetadata):
		return IRODSAccessLevelReadMetadata
	case string(IRODSAccessLevelReadObject), "read":
		return IRODSAccessLevelReadObject
	case string(IRODSAccessLevelWriteAnnotation):
		return IRODSAccessLevelWriteAnnotation
	case string(IRODSAccessLevelCreateMetadata):
		return IRODSAccessLevelCreateMetadata
	case string(IRODSAccessLevelModifyMetadata):
		return IRODSAccessLevelModifyMetadata
	case string(IRODSAccessLevelDeleteMetadata):
		return IRODSAccessLevelDeleteMetadata
	case string(IRODSAccessLevelAdministerObject):
		return IRODSAccessLevelAdministerObject
	case string(IRODSAccessLevelCreateObject), "create":
		return IRODSAccessLevelCreateObject
	case string(IRODSAccessLevelModifyObject), "modify", "write":
		return IRODSAccessLevelModifyObject
	case string(IRODSAccessLevelDeleteObject), "delete":
		return IRODSAccessLevelDeleteObject
	case string(IRODSAccessLevelCreateToken):
		return IRODSAccessLevelCreateToken
	case string(IRODSAccessLevelDeleteToken):
		return IRODSAccessLevelDeleteToken
	case string(IRODSAccessLevelCurate):
		return IRODSAccessLevelCurate
	case string(IRODSAccessLevelOwner):
		return IRODSAccessLevelOwner
	case string(IRODSAccessLevelNull):
		fallthrough
	default:
		return IRODSAccessLevelNull
	}
}

// ChmodString returns the string for update access control messages.
func (accessType IRODSAccessLevelType) ChmodString() string {
	switch accessType {
	case IRODSAccessLevelReadObject:
		return "read"
	case IRODSAccessLevelModifyObject:
		return "write"
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

// IRODSAccessInheritance contains irods access inheritance information
type IRODSAccessInheritance struct {
	Path        string
	Inheritance bool
}

// ToString stringifies the object
func (inheritance *IRODSAccessInheritance) ToString() string {
	return fmt.Sprintf("<IRODSAccessInheritance %s %t>", inheritance.Path, inheritance.Inheritance)
}
