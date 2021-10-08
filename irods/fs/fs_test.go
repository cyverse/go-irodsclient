package fs

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
	conn    *connection.IRODSConnection
)

func setup() {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "setup",
	})

	yaml, err := ioutil.ReadFile("../../config/test_account.yml")
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	timeout = time.Second * 200 // 200 sec

	account.ClientServerNegotiation = false
	logger.Debugf("Account : %v", account.MaskSensitiveData())

	conn = connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err = conn.Connect()
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}
}

func shutdown() {
	conn.Disconnect()
	conn = nil
}

func TestGetIRODSCollection(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestGetIRODSCollection",
	})

	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	logger.Debugf("Collection : %v", collection)

	shutdown()
}

func TestListIRODSCollections(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSCollections",
	})

	setup()

	collections, err := ListSubCollections(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(collections) == 0 {
		logger.Debug("There is no sub collections")
	} else {
		for _, collection := range collections {
			logger.Debugf("Collection : %v", collection)
		}
	}

	shutdown()
}

func TestListManyIRODSCollections(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListManyIRODSCollections",
	})

	setup()

	collections, err := ListSubCollections(conn, "/iplant/home")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(collections) == 0 {
		logger.Debug("There is no sub collections")
	} else {
		for _, collection := range collections {
			logger.Debugf("Collection : %v", collection)
		}
	}

	shutdown()
}

func TestListIRODSCollectionMeta(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSCollectionMeta",
	})

	setup()

	metas, err := ListCollectionMeta(conn, "/iplant/home/iyhoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(metas) == 0 {
		logger.Debug("There is no metadata")
	} else {
		for _, meta := range metas {
			logger.Debugf("Collection Meta : %v", meta)
		}
	}

	shutdown()
}

func TestListIRODSCollectionAccess(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSCollectionAccess",
	})

	setup()

	accesses, err := ListCollectionAccess(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(accesses) == 0 {
		logger.Debug("There is no accesses")
	} else {
		for _, access := range accesses {
			logger.Debugf("Collection Access : %v", access)
		}
	}

	shutdown()
}

func TestListIRODSDataObjects(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSDataObjects",
	})

	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	logger.Debugf("Collection: %v", collection)

	dataobjects, err := ListDataObjects(conn, collection)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	for _, dataobject := range dataobjects {
		logger.Debugf("DataObject : %v", dataobject)
		for _, replica := range dataobject.Replicas {
			logger.Debugf("Replica : %v", replica)
		}
	}

	shutdown()
}

func TestListIRODSDataObjectsMasterReplica(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSDataObjectsMasterReplica",
	})

	setup()

	collection, err := GetCollection(conn, "/iplant/home/iychoi")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	logger.Debugf("Collection: %v", collection)

	dataobjects, err := ListDataObjectsMasterReplica(conn, collection)
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	for _, dataobject := range dataobjects {
		logger.Debugf("DataObject : %v", dataobject)
		for _, replica := range dataobject.Replicas {
			logger.Debugf("Replica : %v", replica)
		}
	}

	shutdown()
}

func TestGetIRODSDataObject(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestGetIRODSDataObject",
	})

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

	logger.Debugf("DataObject : %v", dataobject)
	for _, replica := range dataobject.Replicas {
		logger.Debugf("Replica : %v", replica)
	}

	shutdown()
}

func TestGetIRODSDataObjectMasterReplica(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestGetIRODSDataObjectMasterReplica",
	})

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

	logger.Debugf("DataObject : %v", dataobject)
	for _, replica := range dataobject.Replicas {
		logger.Debugf("Replica : %v", replica)
	}

	shutdown()
}

func TestListIRODSDataObjectMeta(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSDataObjectMeta",
	})

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
		logger.Debug("There is no metadata")
	} else {
		for _, meta := range metas {
			logger.Debugf("Data Object Meta : %v", meta)
		}
	}

	shutdown()
}

func TestListIRODSDataObjectAccess(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSDataObjectAccess",
	})

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
		logger.Debug("There is no accesses")
	} else {
		for _, access := range accesses {
			logger.Debugf("Data Object Access : %v", access)
		}
	}

	shutdown()
}

func TestCreateDeleteIRODSCollection(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestCreateDeleteIRODSCollection",
	})

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

	_, err = GetCollection(conn, "/iplant/home/iychoi/test123")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			logger.Debugf("Deleted collection")
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
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestCreateMoveDeleteIRODSCollection",
	})

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

	_, err = GetCollection(conn, "/iplant/home/iychoi/test456")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			logger.Debugf("Deleted collection")
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
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestCreateDeleteIRODSDataObject",
	})

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

	_, err = GetDataObject(conn, collection, "testobj123")
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			logger.Debugf("Deleted data object")
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
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestListIRODSGroupUsers",
	})

	setup()

	users, err := ListGroupUsers(conn, "rodsadmin")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	if len(users) == 0 {
		logger.Debug("There is no users in the group")
	} else {
		for _, user := range users {
			logger.Debugf("User : %v", user)
		}
	}

	shutdown()
}

func TestSearchDataObjectsByMeta(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestSearchDataObjectsByMeta",
	})

	setup()

	dataobjects, err := SearchDataObjectsByMeta(conn, "ipc_UUID", "3241af9a-c199-11e5-bd90-3c4a92e4a804")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	for _, dataobject := range dataobjects {
		logger.Debugf("DataObject : %v", dataobject)
		for _, replica := range dataobject.Replicas {
			logger.Debugf("Replica : %v", replica)
		}
	}

	shutdown()
}

func TestSearchDataObjectsByMetaWildcard(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "TestSearchDataObjectsByMetaWildcard",
	})

	setup()

	// this takes a long time to perform
	dataobjects, err := SearchDataObjectsByMetaWildcard(conn, "ipc_UUID", "3241af9a-c199-11e5-bd90-3c4a92e4a80%")
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	for _, dataobject := range dataobjects {
		logger.Debugf("DataObject : %v", dataobject)
		for _, replica := range dataobject.Replicas {
			logger.Debugf("Replica : %v", replica)
		}
	}

	shutdown()
}
