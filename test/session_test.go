package test

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"

	log "github.com/sirupsen/logrus"
)

var (
	sessionConfig *session.IRODSSessionConfig
)

func setupSession() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setupSession",
	})

	setupTest()

	account.ClientServerNegotiation = false
	logger.Debugf("Account : %v", account.MaskSensitiveData())

	sessionConfig = session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")
}

func shutdownSession() {
}

func TestSession(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "TestSession",
	})

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

	logger.Debugf("Collection : %v", collection)

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

	logger.Debugf("Collection : %v", collection)

	err = sess.ReturnConnection(conn)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	sess.Release()

	shutdownSession()
}
