package fs

import (
	"fmt"
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

func TestReadWrite(t *testing.T) {
	setup()

	text := "HELLO WORLD!"

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
