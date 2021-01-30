package query

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
)

func init() {
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

	timeout = time.Second * 20 // 20 sec
}

func TestGetIRODSCollection(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection : %v", collection)
}

func TestListIRODSCollections(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collections, err := ListSubCollections(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(collections) == 0 {
		util.LogDebug("There is no sub collections")
	} else {
		for _, collection := range collections {
			util.LogDebugf("Collection : %v", collection)
		}
	}
}

func TestGetIRODSCollectionMeta(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	metas, err := GetCollectionMeta(conn, "/iplant/home/iyhoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(metas) == 0 {
		util.LogDebug("There is no metadata")
	} else {
		for _, meta := range metas {
			util.LogDebugf("Collection Meta : %v", meta)
		}
	}
}

func TestListIRODSDataObjects(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection: %v", collection)

	dataobjects, err := ListDataObjects(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	for _, dataobject := range dataobjects {
		util.LogDebugf("DataObject : %v", dataobject)
		for _, replica := range dataobject.Replicas {
			util.LogDebugf("Replica : %v", replica)
		}
	}
}

func TestGetIRODSDataObjectMeta(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	metas, err := GetDataObjectMeta(conn, "/iplant/home/iyhoi/bench.tmp")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(metas) == 0 {
		util.LogDebug("There is no metadata")
	} else {
		for _, meta := range metas {
			util.LogDebugf("Data Object Meta : %v", meta)
		}
	}
}
