package testcases

import (
	"testing"

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
	//t.Run("ManyConnections", testManyConnections)
	t.Run("Connection", testConnection)
	t.Run("InvalidUsername", testInvalidUsername)
	t.Run("Negotiation", testNegotiation)
}

func testManyConnections(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare

	for i := 0; i < 20; i++ {
		conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
		FailError(t, err)

		err = conn.Connect()
		FailError(t, err)
		defer conn.Disconnect()

		ver := conn.GetVersion()
		verMajor, _, _ := ver.GetReleaseVersion()
		assert.GreaterOrEqual(t, 4, verMajor)

		t.Logf("Connection %d: %s", i, conn.GetVersion().APIVersion)
	}
}

func testConnection(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestDontCare

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	FailError(t, err)

	err = conn.Connect()
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

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func testNegotiation(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()
	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationPolicyRequestTCP

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	FailError(t, err)

	err = conn.Connect()
	FailError(t, err)
	defer conn.Disconnect()

	ver := conn.GetVersion()
	verMajor, _, _ := ver.GetReleaseVersion()
	assert.GreaterOrEqual(t, 4, verMajor)
}
