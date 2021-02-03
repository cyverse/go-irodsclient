package filesystem

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
	conn    *connection.IRODSConnection
	fs      *FileSystem
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

	conn = connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err = conn.Connect()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	fs = NewFileSystem(conn)
}

func shutdown() {
	conn.Disconnect()
	conn = nil
}

func TestListEntries(t *testing.T) {
	setup()

	entries, err := fs.List("/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(entries) == 0 {
		util.LogDebug("There is no entries")
	} else {
		for _, entry := range entries {
			util.LogDebugf("Entry : %v", entry)
		}
	}

	shutdown()
}
