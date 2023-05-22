package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
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

// GetTicketRestrictions gets all restriction info. for the given ticket
func (fs *FileSystem) GetTicketRestrictions(ticketID int64) (*IRODSTicketRestrictions, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	hosts, err := irods_fs.ListTicketAllowedHosts(conn, ticketID)
	if err != nil {
		return nil, err
	}

	usernames, err := irods_fs.ListTicketAllowedUserNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	groupnames, err := irods_fs.ListTicketAllowedGroupNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return &IRODSTicketRestrictions{
		AllowedHosts:      hosts,
		AllowedUserNames:  usernames,
		AllowedGroupNames: groupnames,
	}, nil
}

// ListTicketHostRestrictions lists all host restrictions for the given ticket
func (fs *FileSystem) ListTicketHostRestrictions(ticketID int64) ([]string, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	hosts, err := irods_fs.ListTicketAllowedHosts(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return hosts, err
}

// ListTicketUserNameRestrictions lists all user name restrictions for the given ticket
func (fs *FileSystem) ListTicketUserNameRestrictions(ticketID int64) ([]string, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	usernames, err := irods_fs.ListTicketAllowedUserNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return usernames, err
}

// ListTicketGroupNameRestrictions lists all group name restrictions for the given ticket
func (fs *FileSystem) ListTicketUserGroupRestrictions(ticketID int64) ([]string, error) {
	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metaSession.ReturnConnection(conn)

	groupnames, err := irods_fs.ListTicketAllowedGroupNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return groupnames, err
}

// CreateTicket creates a new ticket
func (fs *FileSystem) CreateTicket(ticketName string, ticketType types.TicketType, path string) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.metaSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metaSession.ReturnConnection(conn)

	err = irods_fs.CreateTicket(conn, ticketName, ticketType, irodsPath)
	if err != nil {
		return err
	}

	return nil
}
