package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// Very secret key that is part of the public cpp code of irods
const (
	scramblePadding string = "1gCBizHWbwIYyWLoysGzTe6SyzqFKMniZX05faZHWAwQKXf6Fs"
)

// AddUser adds a user.
func AddUser(conn *connection.IRODSConnection, username, password string) error {
	// copy the behaviour from setScrambledPw
	if len(password) > common.MaxPasswordLength {
		password = password[0:common.MaxPasswordLength]
	}

	if lencopy := common.MaxPasswordLength - 10 - len(password); lencopy > 15 {
		password = password + scramblePadding[0:lencopy]
	}

	adminPassword := conn.Account.Password

	if conn.Account.AuthenticationScheme == types.AuthSchemePAM {
		adminPassword = conn.GetGeneratedPasswordForPAMAuth()
	}

	scrambledPassword := util.Scramble(password, adminPassword)

	req := message.NewIRODSMessageUserAdminRequest("mkuser", username, scrambledPassword)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// AddGroup adds a group.
func AddGroup(conn *connection.IRODSConnection, group string) error {
	req := message.NewIRODSMessageUserAdminRequest("mkgroup", group, string(types.IRODSUserRodsGroup))

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// AddChildToResc adds a child to a parent resource
func AddChildToResc(conn *connection.IRODSConnection, parent, child, options string) error {
	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}

// AddToGroup adds a user to a group.
func AddToGroup(conn *connection.IRODSConnection, group, user string) error {
	req := message.NewIRODSMessageUserAdminRequest("modify", "group", group, "add", user)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// RmFromGroup removes a user from a group.
func RmFromGroup(conn *connection.IRODSConnection, group, user string) error {
	req := message.NewIRODSMessageUserAdminRequest("modify", "group", group, "remove", user)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// ChangeUserType changes the type / role of a user object
func ChangeUserType(conn *connection.IRODSConnection, user, newType string) error {
	req := message.NewIRODSMessageAdminRequest("modify", "user", user, "type", newType)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}

// RmUser removes a user or a group.
func RmUser(conn *connection.IRODSConnection, user string) error {
	req := message.NewIRODSMessageAdminRequest("rm", "user", user)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}

// SetUserQuota sets quota for a given user and resource ('total' for global)
func SetUserQuota(conn *connection.IRODSConnection, user, resource, value string) error {
	req := message.NewIRODSMessageAdminRequest("set-quota", "user", user, resource, value)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}

// SetGroupQuota sets quota for a given user and resource ('total' for global)
func SetGroupQuota(conn *connection.IRODSConnection, group, resource, value string) error {
	req := message.NewIRODSMessageAdminRequest("set-quota", "group", group, resource, value)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}
