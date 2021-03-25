package fs

import (
	"github.com/thanhpk/randstr"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
)

// AddUser adds a user.
func AddUser(conn *connection.IRODSConnection, username string) error {
	// random scrambled password
	scrambledPassword := randstr.Hex(30)

	req := message.NewIRODSMessageUserAdminRequest("mkuser", username, scrambledPassword)

	return conn.RequestAndCheck(req, &message.IRODSMessageUserAdminResponse{})
}

// AddChildToResc adds a child to a parent resource
func AddChildToResc(conn *connection.IRODSConnection, parent, child, options string) error {
	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	return conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{})
}
