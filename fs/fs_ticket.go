package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// GetTicketForAnonymousAccess gets ticket information for anonymous access
func (fs *FileSystem) GetTicketForAnonymousAccess(ticket string) (*types.IRODSTicketForAnonymousAccess, error) {
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	ticketInfo, err := irods_fs.GetTicketForAnonymousAccess(conn, ticket)
	if err != nil {
		return nil, err
	}

	return ticketInfo, err
}
