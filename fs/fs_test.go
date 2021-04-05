package fs

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

var (
	account *types.IRODSAccount
	fs      *FileSystem
)

func setup() {
	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../config/test_account.yml")
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

	fs, err = NewFileSystemWithDefault(account, "go-irodsclient-test")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}
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

func TestListEntriesByMeta(t *testing.T) {
	setup()

	entries, err := fs.SearchByMeta("ipc_UUID", "3241af9a-c199-11e5-bd90-3c4a92e4a804")
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

func TestListACLs(t *testing.T) {
	setup()

	acls, err := fs.ListACLsWithGroupUsers("/iplant/home/iychoi/all.fna.tar.gz")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(acls) == 0 {
		util.LogDebug("There is no acls")
	} else {
		for _, acl := range acls {
			util.LogDebugf("ACL : %v", acl)
		}
	}

	shutdown()
}

func TestReadWrite(t *testing.T) {
	setup()

	text := "HELLO WORLD!<?!'\">"

	handle, err := fs.CreateFile("/iplant/home/iychoi/testnewfile.txt", "")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	handle.Write([]byte(text))

	handle.Close()

	if !fs.Exists("/iplant/home/iychoi/testnewfile.txt") {
		t.Error("Cannot find the file created")
		panic(fmt.Errorf("Cannot find the file created"))
	}

	newHandle, err := fs.OpenFile("/iplant/home/iychoi/testnewfile.txt", "", "r")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	readData, err := newHandle.Read(1024)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}
	newHandle.Close()

	if string(readData) != text {
		t.Errorf("Wrong data read - %s", string(readData))
		panic(fmt.Errorf("Wrong data read - %s", string(readData)))
	}

	fs.RemoveFile("/iplant/home/iychoi/testnewfile.txt", true)

	if fs.Exists("/iplant/home/iychoi/testnewfile.txt") {
		t.Error("Cannot remove the file created")
		panic(fmt.Errorf("Cannot remove the file created"))
	}

	shutdown()
}
