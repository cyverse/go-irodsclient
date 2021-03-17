package session

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

var (
	account       *types.IRODSAccount
	timeout       time.Duration
	sessionConfig *IRODSSessionConfig
)

func setup() {
	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../../../config/test_account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	timeout = time.Second * 200 // 200 sec

	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	sessionConfig = NewIRODSSessionConfigWithDefault("go-irodsclient-test")
}

func shutdown() {
}

func TestSession(t *testing.T) {
	setup()

	sess, err := NewIRODSSession(account, sessionConfig)
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

	shutdown()
}
