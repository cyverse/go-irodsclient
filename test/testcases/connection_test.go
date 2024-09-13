package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func TestIRODSConnection(t *testing.T) {
	setup()
	defer shutdown()

	t.Run("test IRODS Connection", testIRODSConnection)
	t.Run("test IRODS Invalid Username", testIRODSInvalidUsername)
	t.Run("test IRODS Connection with Negotiation", testIRODSConnectionWithNegotiation)
}

func testIRODSConnection(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)
}

func testIRODSInvalidUsername(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare
	account.ProxyUser = "test$def"
	account.ClientUser = ""

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	assert.Error(t, err)
	fmt.Println(err.Error())
	defer conn.Disconnect()
}

func testIRODSConnectionWithNegotiation(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestTCP

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)
}
