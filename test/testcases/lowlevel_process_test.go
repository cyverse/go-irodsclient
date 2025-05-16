package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/stretchr/testify/assert"
)

func getLowlevelProcessTest() Test {
	return Test{
		Name: "Lowlevel_Process",
		Func: lowlevelProcessTest,
	}
}

func lowlevelProcessTest(t *testing.T, test *Test) {
	t.Run("ProcessStat", testProcessStat)
}

func testProcessStat(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	FailError(t, err)

	err = conn.Connect()
	FailError(t, err)
	defer conn.Disconnect()

	processes, err := fs.StatProcess(conn, "", "")
	FailError(t, err)

	assert.GreaterOrEqual(t, len(processes), 1)
}
