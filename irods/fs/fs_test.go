package fs

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
	conn    *connection.IRODSConnection
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

func TestListIRODSCollectionMeta(t *testing.T) {
	setup()

	metas, err := ListCollectionMeta(conn, "/iplant/home/iyhoi")
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

func TestListIRODSCollectionAccess(t *testing.T) {
	setup()

	accesses, err := ListCollectionAccess(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(accesses) == 0 {
		util.LogDebug("There is no accesses")
	} else {
		for _, access := range accesses {
			util.LogDebugf("Collection Access : %v", access)
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

func TestListIRODSDataObjectMeta(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	metas, err := ListDataObjectMeta(conn, collection, "all.fna.tar.gz")
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

func TestListIRODSDataObjectAccess(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	accesses, err := ListDataObjectAccess(conn, collection, "bench.tmp")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(accesses) == 0 {
		util.LogDebug("There is no accesses")
	} else {
		for _, access := range accesses {
			util.LogDebugf("Data Object Access : %v", access)
		}
	}

	shutdown()
}

func TestCreateDeleteIRODSCollection(t *testing.T) {
	setup()

	err := CreateCollection(conn, "/iplant/home/iychoi/test123", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := GetCollection(conn, "/iplant/home/iychoi/test123")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if collection.ID <= 0 {
		t.Errorf("err - cannot create a collection")
		panic(err)
	}

	err = DeleteCollection(conn, "/iplant/home/iychoi/test123", true, false)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err = GetCollection(conn, "/iplant/home/iychoi/test123")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			util.LogDebugf("Deleted collection")
			deleted = true
		}
	}

	if !deleted {
		// error must occur
		t.Errorf("err - cannot delete a collection")
		panic(err)
	}

	shutdown()
}

func TestCreateMoveDeleteIRODSCollection(t *testing.T) {
	setup()

	err := CreateCollection(conn, "/iplant/home/iychoi/test123", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := GetCollection(conn, "/iplant/home/iychoi/test123")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if collection.ID <= 0 {
		t.Errorf("err - cannot create a collection")
		panic(err)
	}

	err = MoveCollection(conn, "/iplant/home/iychoi/test123", "/iplant/home/iychoi/test456")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err = GetCollection(conn, "/iplant/home/iychoi/test456")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if collection.ID <= 0 {
		t.Errorf("err - cannot move a collection")
		panic(err)
	}

	err = DeleteCollection(conn, "/iplant/home/iychoi/test456", true, false)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err = GetCollection(conn, "/iplant/home/iychoi/test456")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			util.LogDebugf("Deleted collection")
			deleted = true
		}
	}

	if !deleted {
		// error must occur
		t.Errorf("err - cannot delete a collection")
		panic(err)
	}

	shutdown()
}

func TestCreateDeleteIRODSDataObject(t *testing.T) {
	setup()

	handle, err := CreateDataObject(conn, "/iplant/home/iychoi/testobj123", "", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	err = CloseDataObject(conn, handle)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	obj, err := GetDataObject(conn, collection, "testobj123")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if obj.ID <= 0 {
		t.Errorf("err - cannot create a data object")
		panic(err)
	}

	err = DeleteDataObject(conn, "/iplant/home/iychoi/testobj123", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	obj, err = GetDataObject(conn, collection, "testobj123")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			util.LogDebugf("Deleted data object")
			deleted = true
		}
	}

	if !deleted {
		// error must occur
		t.Errorf("err - cannot delete a data object")
		panic(err)
	}

	shutdown()
}

func TestReadWriteIRODSDataObject(t *testing.T) {
	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	handle, err := CreateDataObject(conn, "/iplant/home/iychoi/testobjwrite123", "", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	data := "Hello World"
	err = WriteDataObject(conn, handle, []byte(data))
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	err = CloseDataObject(conn, handle)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	obj, err := GetDataObject(conn, collection, "testobjwrite123")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if obj.ID <= 0 {
		t.Errorf("err - cannot create a data object")
		panic(err)
	}

	handle, _, err = OpenDataObject(conn, "/iplant/home/iychoi/testobjwrite123", "", "r")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	datarecv, err := ReadDataObject(conn, handle, len(data))
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	fmt.Printf("Wrote: %s\n", data)
	fmt.Printf("Read: %s\n", datarecv)

	if data != string(datarecv) {
		t.Error("data does not match")
	}

	err = CloseDataObject(conn, handle)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	err = DeleteDataObject(conn, "/iplant/home/iychoi/testobjwrite123", true)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	shutdown()
}

func TestListIRODSGroupUsers(t *testing.T) {
	setup()

	users, err := ListGroupUsers(conn, "rodsadmin")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(users) == 0 {
		util.LogDebug("There is no users in the group")
	} else {
		for _, user := range users {
			util.LogDebugf("User : %v", user)
		}
	}

	shutdown()
}

func TestSearchDataObjectsByMeta(t *testing.T) {
	setup()

	dataobjects, err := SearchDataObjectsByMeta(conn, "ipc_UUID", "3241af9a-c199-11e5-bd90-3c4a92e4a804")
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

func TestSearchDataObjectsByMetaWildcard(t *testing.T) {
	setup()

	// this takes a long time to perform
	dataobjects, err := SearchDataObjectsByMetaWildcard(conn, "ipc_UUID", "3241af9a-c199-11e5-bd90-3c4a92e4a80%")
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
