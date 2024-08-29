package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	ticketTestID = xid.New().String()
)

func TestTicket(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, ticketTestID)

	t.Run("test PrepareSamples", testPrepareSamplesForTicket)
	t.Run("test CreateAndRemoveTickets", testCreateAndRemoveTickets)
	t.Run("test UpdateTicket", testUpdateTicket)
}

func testPrepareSamplesForTicket(t *testing.T) {
	prepareSamples(t, ticketTestID)
}

func testCreateAndRemoveTickets(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ticketTestID)

	entries, err := filesystem.List(homedir)
	failError(t, err)

	collectionPaths := []string{}
	dataObjectPaths := []string{}

	for _, entry := range entries {
		assert.NotEmpty(t, entry.ID)

		if entry.Type == fs.DirectoryEntry {
			collectionPaths = append(collectionPaths, entry.Path)
		} else {
			dataObjectPaths = append(dataObjectPaths, entry.Path)
		}
	}

	assert.GreaterOrEqual(t, len(collectionPaths), 3)
	lenDir := len(collectionPaths)
	if lenDir > 3 {
		lenDir = 3
	}

	assert.GreaterOrEqual(t, len(dataObjectPaths), 3)
	lenFile := len(dataObjectPaths)
	if lenFile > 3 {
		lenFile = 3
	}

	// create 3 tickets for collections
	for i := 0; i < lenDir; i++ {
		ticketName := xid.New().String()
		err = filesystem.CreateTicket(ticketName, types.TicketTypeWrite, collectionPaths[i])
		failError(t, err)
	}

	// create 3 tickets for data objects
	for i := 0; i < lenFile; i++ {
		ticketName := xid.New().String()
		err = filesystem.CreateTicket(ticketName, types.TicketTypeRead, dataObjectPaths[i])
		failError(t, err)
	}

	tickets, err := filesystem.ListTickets()
	failError(t, err)

	assert.Equal(t, lenDir+lenFile, len(tickets))

	for _, ticket := range tickets {
		if ticket.ObjectType == types.ObjectTypeDataObject {
			assert.Equal(t, types.TicketTypeRead, ticket.Type)
		} else {
			assert.Equal(t, types.TicketTypeWrite, ticket.Type)
		}
	}

	// remove
	for _, ticket := range tickets {
		err = filesystem.DeleteTicket(ticket.Name)
		failError(t, err)
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	assert.Equal(t, 0, len(tickets))
}

func testUpdateTicket(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(ticketTestID)

	entries, err := filesystem.List(homedir)
	failError(t, err)

	collectionPaths := []string{}

	for _, entry := range entries {
		assert.NotEmpty(t, entry.ID)

		if entry.Type == fs.DirectoryEntry {
			collectionPaths = append(collectionPaths, entry.Path)
		}
	}

	assert.GreaterOrEqual(t, len(collectionPaths), 1)

	// create a ticket for a collections
	ticketName := xid.New().String()
	err = filesystem.CreateTicket(ticketName, types.TicketTypeWrite, collectionPaths[0])
	failError(t, err)

	tickets, err := filesystem.ListTickets()
	failError(t, err)

	assert.Equal(t, 1, len(tickets))

	for _, ticket := range tickets {
		assert.Equal(t, types.ObjectTypeCollection, ticket.ObjectType)
		assert.Equal(t, types.TicketTypeWrite, ticket.Type)
		assert.True(t, ticket.ExpirationTime.IsZero())
		assert.Equal(t, int64(0), ticket.UsesLimit)
		assert.Equal(t, int64(10), ticket.WriteFileLimit)
		assert.Equal(t, int64(0), ticket.WriteByteLimit)
	}

	// update - expiration time
	for _, ticket := range tickets {
		filesystem.ModifyTicketExpirationTime(ticket.Name, time.Now().Add(1*time.Hour))
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	for _, ticket := range tickets {
		assert.True(t, ticket.ExpirationTime.After(time.Now()))
	}

	// clear - expiration time
	for _, ticket := range tickets {
		filesystem.ClearTicketExpirationTime(ticket.Name)
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	for _, ticket := range tickets {
		assert.True(t, ticket.ExpirationTime.IsZero())
	}

	// update - limit
	for _, ticket := range tickets {
		filesystem.ModifyTicketUseLimit(ticket.Name, 100)
		filesystem.ModifyTicketWriteByteLimit(ticket.Name, 101)
		filesystem.ModifyTicketWriteFileLimit(ticket.Name, 102)
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	for _, ticket := range tickets {
		assert.Equal(t, int64(100), ticket.UsesLimit)
		assert.Equal(t, int64(101), ticket.WriteByteLimit)
		assert.Equal(t, int64(102), ticket.WriteFileLimit)
	}

	// clear - limit
	for _, ticket := range tickets {
		filesystem.ClearTicketUseLimit(ticket.Name)
		filesystem.ClearTicketWriteByteLimit(ticket.Name)
		filesystem.ClearTicketWriteFileLimit(ticket.Name)
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	for _, ticket := range tickets {
		assert.Equal(t, int64(0), ticket.UsesLimit)
		assert.Equal(t, int64(0), ticket.WriteByteLimit)
		assert.Equal(t, int64(0), ticket.WriteFileLimit)
	}

	// remove
	for _, ticket := range tickets {
		err = filesystem.DeleteTicket(ticket.Name)
		failError(t, err)
	}

	tickets, err = filesystem.ListTickets()
	failError(t, err)

	assert.Equal(t, 0, len(tickets))
}
