package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
)

func TestIRODSSession(t *testing.T) {
	setup()

	t.Run("test IRODS Session", testSession)
	t.Run("test many IRODS Connections", testManyConnections)

	shutdown()
}

func testSession(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationDontCare
	t.Logf("Account : %v", account.MaskSensitiveData())

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	// first
	conn, err := sess.AcquireConnection()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	collection, err := fs.GetCollection(conn, homedir)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	t.Logf("Collection : %v", collection)

	err = sess.ReturnConnection(conn)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	// second
	conn, err = sess.AcquireConnection()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	collection, err = fs.GetCollection(conn, homedir)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	t.Logf("Collection : %v", collection)

	err = sess.ReturnConnection(conn)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	sess.Release()
}

func testManyConnections(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationDontCare
	t.Logf("Account : %v", account.MaskSensitiveData())

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 30; i++ {
		conn, err := sess.AcquireConnection()
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

		collection, err := fs.GetCollection(conn, homedir)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		connections = append(connections, conn)
		t.Logf("Collection %d : %v", i, collection)
	}

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
	}

	sess.Release()
}
