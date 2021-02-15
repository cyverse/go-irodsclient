package fs

import (
	"io/ioutil"
	"testing"

	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

var (
	account *types.IRODSAccount
	fs      *FileSystem
)

func setup() {
	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../../config/test_account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	fs = NewFileSystemWithDefault(account, "go-irodsclient-test")
}

func shutdown() {
	fs.Release()
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
