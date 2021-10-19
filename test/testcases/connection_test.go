package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
)

func TestIRODSConnection(t *testing.T) {
	setup()

	t.Run("test IRODS Connection", testIRODSConnection)
	t.Run("test IRODS Connection with Negotiation", testIRODSConnectionWithNegotiation)

	shutdown()
}

func testIRODSConnection(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationDontCare
	t.Logf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, 300*time.Second, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	ver := conn.GetVersion()
	t.Logf("Version : %v", ver)

	conn.Disconnect()
}

func testIRODSConnectionWithNegotiation(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationRequireTCP
	t.Logf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, 300*time.Second, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	ver := conn.GetVersion()
	t.Logf("Version : %v", ver)

	conn.Disconnect()
}
