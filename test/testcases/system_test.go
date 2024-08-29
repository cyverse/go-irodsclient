package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
)

func TestSystem(t *testing.T) {
	setup()
	defer shutdown()

	t.Run("test ProcessStat", testProcessStat)
}

func testProcessStat(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	processes, err := fs.StatProcess(conn, "", "")
	failError(t, err)

	for _, process := range processes {
		t.Logf("process %q\n", process.ToString())
	}
}
