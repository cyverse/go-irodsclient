package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
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

	conn := connection.NewIRODSConnection(account, 300*time.Second, "go-irodsclient-test")
	err := conn.Connect()
	assert.NoError(t, err)

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)

	conn.Disconnect()
}

func testIRODSConnectionWithNegotiation(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationRequireTCP

	conn := connection.NewIRODSConnection(account, 300*time.Second, "go-irodsclient-test")
	err := conn.Connect()
	assert.NoError(t, err)

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)

	conn.Disconnect()
}
