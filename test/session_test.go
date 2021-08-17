package test

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/util"
)

var (
	sessionConfig *session.IRODSSessionConfig
)

func setupSession() {
	setupTest()

	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	sessionConfig = session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")
}

func shutdownSession() {
}

func TestSession(t *testing.T) {
	setupSession()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	// first
	conn, err := sess.AcquireConnection()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := fs.GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection : %v", collection)

	err = sess.ReturnConnection(conn)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	// second
	conn, err = sess.AcquireConnection()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err = fs.GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection : %v", collection)

	err = sess.ReturnConnection(conn)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	sess.Release()

	shutdownSession()
}
