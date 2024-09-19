package testcases

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	irods_util "github.com/cyverse/go-irodsclient/irods/util"
	"github.com/stretchr/testify/assert"
)

func TestAdmin(t *testing.T) {
	setup()
	defer shutdown()

	t.Run("test EncoderRing", testEncoderRing)
	t.Run("test Scramble", testScramble)
	t.Run("test ClientSignature", testClientSignature)

	t.Run("test CreateAndRemoveUser", testCreateAndRemoveUser)
}

func testEncoderRing(t *testing.T) {
	ring := irods_util.GetEncoderRing("def")
	ringHex := hex.EncodeToString(ring)
	assert.Equal(t, "5fbabc5bfd2ef4f4d65024d364c3241a71c71aae827a91b654e9de55e62f3cb23840a894e36c7149ddd8963a1b228df43840a894e36c7149ddd8963a1b228df4", ringHex)
}

func testScramble(t *testing.T) {
	scrPass1 := irods_util.Scramble(";.ObfV2test_password", "06fed401fb79f864272a421835486736", "", false)
	assert.Equal(t, ";EBo$tJuoAY_RigHonj-", scrPass1)

	scrPass2 := irods_util.Scramble(";.ObfV2test_password", "06fed401fb79f864272a421835486736", "", true)
	assert.Equal(t, ";E3O&GDl4!&_$3GBd+B\"", scrPass2)
}

func testClientSignature(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	signature := conn.GetClientSignature()
	assert.Equal(t, 32, len(signature))
}

func testCreateAndRemoveUser(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	// create
	testUsername := "test_user"
	testPassword := "test_password"

	err = fs.CreateUser(conn, testUsername, account.ClientZone, "rodsuser")
	failError(t, err)

	err = fs.ChangeUserPassword(conn, testUsername, account.ClientZone, testPassword)
	failError(t, err)

	// login test
	userAccount := &types.IRODSAccount{
		AuthenticationScheme:    account.AuthenticationScheme,
		ClientServerNegotiation: account.ClientServerNegotiation,
		CSNegotiationPolicy:     account.CSNegotiationPolicy,
		Host:                    account.Host,
		Port:                    account.Port,
		ClientUser:              testUsername,
		ClientZone:              account.ClientZone,
		ProxyUser:               testUsername,
		ProxyZone:               account.ProxyZone,
		Password:                testPassword,
		DefaultResource:         account.DefaultResource,
		PAMToken:                account.PAMToken,
		PamTTL:                  account.PamTTL,
	}

	userConn := connection.NewIRODSConnection(userAccount, 300*time.Second, GetTestApplicationName())
	err = userConn.Connect()
	failError(t, err)
	userConn.Disconnect()

	// delete
	err = fs.RemoveUser(conn, testUsername, account.ClientZone)
	failError(t, err)

	userConn = connection.NewIRODSConnection(userAccount, 300*time.Second, GetTestApplicationName())
	err = userConn.Connect()
	assert.Error(t, err)
	userConn.Disconnect()
}
