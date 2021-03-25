package fs

import (
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// Maximum password length
const maxPwLength = 50

// Very secret key that is part of the public cpp code of irods
const scramblePadding = "1gCBizHWbwIYyWLoysGzTe6SyzqFKMniZX05faZHWAwQKXf6Fs"

// AddUser adds a user.
func AddUser(conn *connection.IRODSConnection, username, password string) error {
	// copy the behaviour from setScrambledPw
	if len(password) > maxPwLength {
		password = password[0:maxPwLength]
	}

	if lencopy := maxPwLength - 10 - len(password); lencopy > 15 {
		password = password + scramblePadding[0:lencopy]
	}

	adminPassword := conn.Account.Password

	if conn.Account.AuthenticationScheme == types.AuthSchemePAM {
		adminPassword = conn.GeneratedPassword
	}

	scrambledPassword := util.Scramble(password, adminPassword)

	req := message.NewIRODSMessageUserAdminRequest("mkuser", username, scrambledPassword)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// AddChildToResc adds a child to a parent resource
func AddChildToResc(conn *connection.IRODSConnection, parent, child, options string) error {
	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}
