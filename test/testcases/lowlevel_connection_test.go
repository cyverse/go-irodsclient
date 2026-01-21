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
	t.Run("Connection", testConnection)
	t.Run("InvalidUsername", testInvalidUsername)
	t.Run("ManyConnections", testManyConnections)
}

func testConnection(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	conn, err := connection.NewIRODSConnection(account, server.GetConnectionConfig())
	FailError(t, err)

	err = conn.Connect()
	FailError(t, err)
	defer func() {
		_ = conn.Disconnect()
	}()

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
		defer func() {
			_ = conn.Disconnect()
		}()

		ver := conn.GetVersion()
		verMajor, _, _ := ver.GetReleaseVersion()
		assert.GreaterOrEqual(t, 4, verMajor)

		t.Logf("Connection %d: %s %s", i, conn.GetVersion().ReleaseVersion, conn.GetVersion().APIVersion)
	}
}
