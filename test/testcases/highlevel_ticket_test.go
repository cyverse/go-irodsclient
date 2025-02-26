package testcases

import (
	"path"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func getHighlevelTicketTest() Test {
	return Test{
		Name: "Highlevel_Ticket",
		Func: highlevelTicketTest,
	}
}

func highlevelTicketTest(t *testing.T, test *Test) {
	t.Run("CreateAndRemoveTickets", testCreateAndRemoveTickets)
	t.Run("UpdateTicket", testUpdateTicket)
}

func testCreateAndRemoveTickets(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	files, dirs, err := CreateSampleFilesAndDirs(t, server, homeDir, 3, 3)
	FailError(t, err)

	assert.Equal(t, 3, len(files))
	assert.Equal(t, 3, len(dirs))

	// create 3 tickets for collections
	dirTickets := []string{}
	for _, dir := range dirs {
		ticketName := "ticket_dir_" + path.Base(dir)
		err = filesystem.CreateTicket(ticketName, types.TicketTypeWrite, dir)
		FailError(t, err)

		dirTickets = append(dirTickets, ticketName)
	}

	// create 3 tickets for data objects
	fileTickets := []string{}
	for _, file := range files {
		ticketName := "ticket_file_" + path.Base(file)
		err = filesystem.CreateTicket(ticketName, types.TicketTypeRead, file)
		FailError(t, err)

		fileTickets = append(fileTickets, ticketName)
	}

	tickets, err := filesystem.ListTickets()
	FailError(t, err)

	assert.Equal(t, len(dirs)+len(files), len(tickets))

	for _, ticket := range tickets {
		if ticket.ObjectType == types.ObjectTypeDataObject {
			assert.Equal(t, types.TicketTypeRead, ticket.Type)
			assert.Contains(t, fileTickets, ticket.Name)
			assert.Contains(t, files, ticket.Path)
		} else {
			assert.Equal(t, types.TicketTypeWrite, ticket.Type)
			assert.Contains(t, dirTickets, ticket.Name)
			assert.Contains(t, dirs, ticket.Path)
		}
	}

	// remove
	for _, ticket := range tickets {
		err = filesystem.DeleteTicket(ticket.Name)
		FailError(t, err)
	}

	tickets, err = filesystem.ListTickets()
	FailError(t, err)

	// remove files
	for _, file := range files {
		err = filesystem.RemoveFile(file, true)
		FailError(t, err)
	}

	for _, dir := range dirs {
		err = filesystem.RemoveDir(dir, true, true)
		FailError(t, err)
	}

	assert.Equal(t, 0, len(tickets))
}

func testUpdateTicket(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	files, dirs, err := CreateSampleFilesAndDirs(t, server, homeDir, 3, 3)
	FailError(t, err)

	assert.Equal(t, 3, len(files))
	assert.Equal(t, 3, len(dirs))

	// create 3 tickets for collections
	for _, dir := range dirs {
		ticketName := "ticket_dir_" + path.Base(dir)
		err = filesystem.CreateTicket(ticketName, types.TicketTypeWrite, dir)
		FailError(t, err)
	}

	// create 3 tickets for data objects
	for _, file := range files {
		ticketName := "ticket_file_" + path.Base(file)
		err = filesystem.CreateTicket(ticketName, types.TicketTypeRead, file)
		FailError(t, err)
	}

	tickets, err := filesystem.ListTickets()
	FailError(t, err)

	assert.Equal(t, len(dirs)+len(files), len(tickets))

	// update - expiration time
	for _, ticket := range tickets {
		filesystem.ModifyTicketExpirationTime(ticket.Name, time.Now().Add(1*time.Hour))
	}

	tickets, err = filesystem.ListTickets()
	FailError(t, err)

	for _, ticket := range tickets {
		assert.True(t, ticket.ExpirationTime.After(time.Now()))
		assert.True(t, ticket.ExpirationTime.Before(time.Now().Add(2*time.Hour)))
	}

	// clear - expiration time
	for _, ticket := range tickets {
		filesystem.ClearTicketExpirationTime(ticket.Name)
	}

	tickets, err = filesystem.ListTickets()
	FailError(t, err)

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
	FailError(t, err)

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
	FailError(t, err)

	for _, ticket := range tickets {
		assert.Equal(t, int64(0), ticket.UsesLimit)
		assert.Equal(t, int64(0), ticket.WriteByteLimit)
		assert.Equal(t, int64(0), ticket.WriteFileLimit)
	}

	// remove
	for _, ticket := range tickets {
		err = filesystem.DeleteTicket(ticket.Name)
		FailError(t, err)
	}

	tickets, err = filesystem.ListTickets()
	FailError(t, err)

	// remove files
	for _, file := range files {
		err = filesystem.RemoveFile(file, true)
		FailError(t, err)
	}

	for _, dir := range dirs {
		err = filesystem.RemoveDir(dir, true, true)
		FailError(t, err)
	}

	assert.Equal(t, 0, len(tickets))
}
