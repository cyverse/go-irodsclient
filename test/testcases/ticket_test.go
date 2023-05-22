package testcases

import (
	"testing"

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
}

func testPrepareSamplesForTicket(t *testing.T) {
	prepareSamples(t, ticketTestID)
}

func testCreateAndRemoveTickets(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

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

	lenDir := len(collectionPaths)
	if lenDir > 3 {
		lenDir = 3
	}

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
}
