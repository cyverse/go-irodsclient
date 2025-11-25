package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/stretchr/testify/assert"
)

func getLowlevelConnectionTest() Test {
	return Test{
		Name:               "Lowlevel_Connection",
		Func:               lowlevelConnectionTest,
		DoNotCreateHomeDir: true,
	}
}

func lowlevelConnectionTest(t *testing.T, test *Test) {
	t.Run("ManyConnections", testManyConnections)
	t.Run("Connection", testConnection)
	t.Run("InvalidUsername", testInvalidUsername)
}

func testManyConnections(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	for i := 0; i < 10; i++ {
		conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
		FailError(t, err)

		err = conn.Connect()
		FailError(t, err)
		defer conn.Disconnect()

		ver := conn.GetVersion()
		verMajor, _, _ := ver.GetReleaseVersion()
		assert.GreaterOrEqual(t, 4, verMajor)

		t.Logf("Connection %d: %s %s", i, conn.GetVersion().ReleaseVersion, conn.GetVersion().APIVersion)
	}
}

func testConnection(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	t.Logf("account info: %+v", account)

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
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)
	account.ProxyUser = "test$def"
	account.ClientUser = ""

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	assert.Error(t, err)
	assert.Nil(t, conn)
}
