package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// GetTicketForAnonymousAccess gets ticket information for anonymous access
func (fs *FileSystem) GetTicketForAnonymousAccess(ticket string) (*types.IRODSTicketForAnonymousAccess, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	ticketInfo, err := irods_fs.GetTicketForAnonymousAccess(conn, ticket)
	if err != nil {
		return nil, err
	}

	return ticketInfo, err
}

// ListTickets lists all available ticket information
func (fs *FileSystem) ListTickets() ([]*types.IRODSTicket, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	tickets, err := irods_fs.ListTickets(conn)
	if err != nil {
		return nil, err
	}

	return tickets, err
}

// ListTicketsBasic lists all available basic ticket information
func (fs *FileSystem) ListTicketsBasic() ([]*types.IRODSTicket, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	tickets, err := irods_fs.ListTicketsBasic(conn)
	if err != nil {
		return nil, err
	}

	return tickets, err
}
