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
	conn    *connection.IRODSConnection
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
}

func shutdown() {
	conn.Disconnect()
	conn = nil
}

func TestGetIRODSCollection(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection : %v", collection)

	shutdown()
}

func TestListIRODSCollections(t *testing.T) {
	setup()

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

	shutdown()
}

func TestListManyIRODSCollections(t *testing.T) {
	setup()

	collections, err := ListSubCollections(conn, "/iplant/home")
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

	shutdown()
}

func TestGetIRODSCollectionMeta(t *testing.T) {
	setup()

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

	shutdown()
}

func TestListIRODSDataObjects(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection: %v", collection)

	dataobjects, err := ListDataObjects(conn, collection)
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

	shutdown()
}

func TestListIRODSDataObjectsMasterReplica(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Collection: %v", collection)

	dataobjects, err := ListDataObjectsMasterReplica(conn, collection)
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

	shutdown()
}

func TestGetIRODSDataObject(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	dataobject, err := GetDataObject(conn, collection, "bench.tmp")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("DataObject : %v", dataobject)
	for _, replica := range dataobject.Replicas {
		util.LogDebugf("Replica : %v", replica)
	}

	shutdown()
}

func TestGetIRODSDataObjectMasterReplica(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	dataobject, err := GetDataObjectMasterReplica(conn, collection, "bench.tmp")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("DataObject : %v", dataobject)
	for _, replica := range dataobject.Replicas {
		util.LogDebugf("Replica : %v", replica)
	}

	shutdown()
}

func TestGetIRODSDataObjectMeta(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	metas, err := GetDataObjectMeta(conn, collection, "bench.tmp")
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

	shutdown()
}

func TestCreateIRODSCollection(t *testing.T) {
	setup()

	err := CreateCollection(conn, "/iplant/home/iychoi/test123", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	shutdown()
}
