package fs

import (
	"time"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// GetTicketForAnonymousAccess gets ticket information for anonymous access
func (fs *FileSystem) GetTicketForAnonymousAccess(ticketName string) (*types.IRODSTicketForAnonymousAccess, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	ticketInfo, err := irods_fs.GetTicketForAnonymousAccess(conn, ticketName)
	if err != nil {
		return nil, err
	}

	return ticketInfo, err
}

// GetTicket gets ticket information
func (fs *FileSystem) GetTicket(ticketName string) (*types.IRODSTicket, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	ticketInfo, err := irods_fs.GetTicket(conn, ticketName)
	if err != nil {
		return nil, err
	}

	return ticketInfo, err
}

// ListTickets lists all available ticket information
func (fs *FileSystem) ListTickets() ([]*types.IRODSTicket, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	tickets, err := irods_fs.ListTickets(conn)
	if err != nil {
		return nil, err
	}

	return tickets, err
}

// ListTicketsBasic lists all available basic ticket information
func (fs *FileSystem) ListTicketsBasic() ([]*types.IRODSTicket, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	tickets, err := irods_fs.ListTicketsBasic(conn)
	if err != nil {
		return nil, err
	}

	return tickets, err
}

// GetTicketRestrictions gets all restriction info. for the given ticket
func (fs *FileSystem) GetTicketRestrictions(ticketID int64) (*IRODSTicketRestrictions, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

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
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	hosts, err := irods_fs.ListTicketAllowedHosts(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return hosts, err
}

// ListTicketUserNameRestrictions lists all user name restrictions for the given ticket
func (fs *FileSystem) ListTicketUserNameRestrictions(ticketID int64) ([]string, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	usernames, err := irods_fs.ListTicketAllowedUserNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return usernames, err
}

// ListTicketUserGroupRestrictions lists all group name restrictions for the given ticket
func (fs *FileSystem) ListTicketUserGroupRestrictions(ticketID int64) ([]string, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupnames, err := irods_fs.ListTicketAllowedGroupNames(conn, ticketID)
	if err != nil {
		return nil, err
	}

	return groupnames, err
}

// CreateTicket creates a new ticket
func (fs *FileSystem) CreateTicket(ticketName string, ticketType types.TicketType, path string) error {
	irodsPath := util.GetCorrectIRODSPath(path)

	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.CreateTicket(conn, ticketName, ticketType, irodsPath)
	if err != nil {
		return err
	}

	return nil
}

// DeleteTicket deletes the given ticket
func (fs *FileSystem) DeleteTicket(ticketName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.DeleteTicket(conn, ticketName)
	if err != nil {
		return err
	}

	return nil
}

// ModifyTicketUseLimit modifies the use limit of the given ticket
func (fs *FileSystem) ModifyTicketUseLimit(ticketName string, uses int64) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ModifyTicketUseLimit(conn, ticketName, uses)
	if err != nil {
		return err
	}

	return nil
}

// ClearTicketUseLimit clears the use limit of the given ticket
func (fs *FileSystem) ClearTicketUseLimit(ticketName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ClearTicketUseLimit(conn, ticketName)
	if err != nil {
		return err
	}

	return nil
}

// ModifyTicketWriteFileLimit modifies the write file limit of the given ticket
func (fs *FileSystem) ModifyTicketWriteFileLimit(ticketName string, count int64) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ModifyTicketWriteFileLimit(conn, ticketName, count)
	if err != nil {
		return err
	}

	return nil
}

// ClearTicketWriteFileLimit clears the write file limit of the given ticket
func (fs *FileSystem) ClearTicketWriteFileLimit(ticketName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ClearTicketWriteFileLimit(conn, ticketName)
	if err != nil {
		return err
	}

	return nil
}

// ModifyTicketWriteByteLimit modifies the write byte limit of the given ticket
func (fs *FileSystem) ModifyTicketWriteByteLimit(ticketName string, bytes int64) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ModifyTicketWriteByteLimit(conn, ticketName, bytes)
	if err != nil {
		return err
	}

	return nil
}

// ClearTicketWriteByteLimit clears the write byte limit of the given ticket
func (fs *FileSystem) ClearTicketWriteByteLimit(ticketName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ClearTicketWriteByteLimit(conn, ticketName)
	if err != nil {
		return err
	}

	return nil
}

// AddTicketAllowedUser adds a user to the allowed user names list of the given ticket
func (fs *FileSystem) AddTicketAllowedUser(ticketName string, userName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddTicketAllowedUser(conn, ticketName, userName)
	if err != nil {
		return err
	}

	return nil
}

// RemoveTicketAllowedUser removes the user from the allowed user names list of the given ticket
func (fs *FileSystem) RemoveTicketAllowedUser(ticketName string, userName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.RemoveTicketAllowedUser(conn, ticketName, userName)
	if err != nil {
		return err
	}

	return nil
}

// AddTicketAllowedGroup adds a group to the allowed group names list of the given ticket
func (fs *FileSystem) AddTicketAllowedGroup(ticketName string, groupName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddTicketAllowedGroup(conn, ticketName, groupName)
	if err != nil {
		return err
	}

	return nil
}

// RemoveTicketAllowedGroup removes the group from the allowed group names list of the given ticket
func (fs *FileSystem) RemoveTicketAllowedGroup(ticketName string, groupName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.RemoveTicketAllowedGroup(conn, ticketName, groupName)
	if err != nil {
		return err
	}

	return nil
}

// AddTicketAllowedHost adds a host to the allowed hosts list of the given ticket
func (fs *FileSystem) AddTicketAllowedHost(ticketName string, host string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddTicketAllowedHost(conn, ticketName, host)
	if err != nil {
		return err
	}

	return nil
}

// RemoveTicketAllowedHost removes the host from the allowed hosts list of the given ticket
func (fs *FileSystem) RemoveTicketAllowedHost(ticketName string, host string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.RemoveTicketAllowedHost(conn, ticketName, host)
	if err != nil {
		return err
	}

	return nil
}

// ModifyTicketExpirationTime modifies the expiration time of the given ticket
func (fs *FileSystem) ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ModifyTicketExpirationTime(conn, ticketName, expirationTime)
	if err != nil {
		return err
	}

	return nil
}

// ClearTicketExpirationTime clears the expiration time of the given ticket
func (fs *FileSystem) ClearTicketExpirationTime(ticketName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ClearTicketExpirationTime(conn, ticketName)
	if err != nil {
		return err
	}

	return nil
}
