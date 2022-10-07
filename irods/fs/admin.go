package fs

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// CreateUser creates a user.
func CreateUser(conn *connection.IRODSConnection, username string, zone string, userType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	req := message.NewIRODSMessageAdminRequest("add", "user", userZoneName, userType, zone)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}

// ChangeUserPassword changes the password of a user object
func ChangeUserPassword(conn *connection.IRODSConnection, username string, zone string, newPassword string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	account := conn.GetAccount()

	oldPassword := account.Password
	if account.AuthenticationScheme == types.AuthSchemePAM {
		oldPassword = conn.GetGeneratedPasswordForPAMAuth()
	}

	scrambledPassword := util.ObfuscateNewPassword(newPassword, oldPassword, conn.GetClientSignature())

	req := message.NewIRODSMessageAdminRequest("modify", "user", userZoneName, "password", scrambledPassword, zone)

	return conn.RequestAndCheckForPassword(req, &message.IRODSMessageAdminResponse{}, nil)
}

// ChangeUserType changes the type / role of a user object
func ChangeUserType(conn *connection.IRODSConnection, username string, zone string, newType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	req := message.NewIRODSMessageAdminRequest("modify", "user", userZoneName, "type", newType, zone)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}

// RemoveUser removes a user or a group.
func RemoveUser(conn *connection.IRODSConnection, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("rm", "user", username, zone)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}

// CreateGroup creates a group.
func CreateGroup(conn *connection.IRODSConnection, groupname string, groupType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("add", "user", groupname, groupType)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{}, nil)
}

// AddGroupMember adds a user to a group.
func AddGroupMember(conn *connection.IRODSConnection, groupname string, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("modify", "group", groupname, "add", username, zone)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{}, nil)
}

// RemoveGroupMember removes a user from a group.
func RemoveGroupMember(conn *connection.IRODSConnection, groupname string, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("modify", "group", groupname, "remove", username, zone)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{}, nil)
}

// AddChildToResc adds a child to a parent resource
func AddChildToResc(conn *connection.IRODSConnection, parent string, child string, options string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}

// SetUserQuota sets quota for a given user and resource ('total' for global)
func SetUserQuota(conn *connection.IRODSConnection, user string, resource string, value string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("set-quota", "user", user, resource, value)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}

// SetGroupQuota sets quota for a given user and resource ('total' for global)
func SetGroupQuota(conn *connection.IRODSConnection, group string, resource string, value string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("set-quota", "group", group, resource, value)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
}
