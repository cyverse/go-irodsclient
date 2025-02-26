package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func getLowlevelConnectionTest() Test {
	return Test{
		Name: "Lowlevel_Connection",
		Func: lowlevelConnectionTest,
	}
}

func lowlevelConnectionTest(t *testing.T, test *Test) {
	t.Run("Connection", testConnection)
	t.Run("InvalidUsername", testInvalidUsername)
	t.Run("Negotiation", testNegotiation)
}

func testConnection(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare

	conn := connection.NewIRODSConnection(account, 300*time.Second, server.GetApplicationName())
	err := conn.Connect()
	FailError(t, err)
	defer conn.Disconnect()

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)
}

func testInvalidUsername(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare
	account.ProxyUser = "test$def"
	account.ClientUser = ""

	conn := connection.NewIRODSConnection(account, 300*time.Second, server.GetApplicationName())
	err := conn.Connect()
	assert.Error(t, err)
	fmt.Println(err.Error())
	defer conn.Disconnect()
}

func testNegotiation(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestTCP

	conn := connection.NewIRODSConnection(account, 300*time.Second, server.GetApplicationName())
	err := conn.Connect()
	FailError(t, err)
	defer conn.Disconnect()

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)
}
