package testcases

import (
	"sync"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	lockAPITestID = xid.New().String()
)

func TestLockAPI(t *testing.T) {
	setup()
	defer shutdown()

	log.SetLevel(log.DebugLevel)

	makeHomeDir(t, lockAPITestID)

	t.Run("test PrepareSamples", testPrepareSamplesForLockAPI)
	t.Run("test SimpleLockIRODSDataObject", testSimpleLockIRODSDataObject)
}

func testPrepareSamplesForLockAPI(t *testing.T) {
	prepareSamples(t, lockAPITestID)
}

func testSimpleLockIRODSDataObject(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	conn := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err := conn.Connect()
	failError(t, err)
	defer conn.Disconnect()

	conn2 := connection.NewIRODSConnection(account, 300*time.Second, GetTestApplicationName())
	err = conn2.Connect()
	failError(t, err)
	defer conn2.Disconnect()

	homedir := getHomeDir(lockAPITestID)

	// create
	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	taskOrder := []int{}
	wg := sync.WaitGroup{}

	keywords := map[common.KeyWord]string{}

	wg.Add(1)
	go func() {
		handle, err := fs.CreateDataObject(conn, newDataObjectPath, "", "w", true, keywords)
		failError(t, err)

		// lock
		t.Log("try lock 1")
		taskOrder = append(taskOrder, 1)
		lockHandle, err := fs.LockDataObject(conn, newDataObjectPath, types.DataObjectLockTypeWrite, types.DataObjectLockCommandSetLockWait)
		failError(t, err)
		t.Log("locked 1")
		taskOrder = append(taskOrder, 2)

		wg.Add(1)
		go func() {
			// lock2 - must wait
			t.Log("try lock 2")
			taskOrder = append(taskOrder, 3)
			lockHandle2, err := fs.LockDataObject(conn2, newDataObjectPath, types.DataObjectLockTypeWrite, types.DataObjectLockCommandSetLockWait)
			failError(t, err)
			t.Log("locked 2")
			taskOrder = append(taskOrder, 6)

			// unlock
			t.Log("try unlock 2")
			taskOrder = append(taskOrder, 7)
			err = fs.UnlockDataObject(conn2, lockHandle2)
			failError(t, err)
			t.Log("unlocked 2")
			taskOrder = append(taskOrder, 8)

			wg.Done()
		}()

		// sleep 5 sec
		time.Sleep(5 * time.Second)

		err = fs.WriteDataObject(conn, handle, []byte("hello world"))
		failError(t, err)

		// unlock
		t.Log("try unlock 1")
		taskOrder = append(taskOrder, 4)
		err = fs.UnlockDataObject(conn, lockHandle)
		failError(t, err)
		t.Log("unlocked 1")
		taskOrder = append(taskOrder, 5)

		err = fs.CloseDataObject(conn, handle)
		failError(t, err)

		wg.Done()
	}()

	wg.Wait()

	assert.ElementsMatch(t, taskOrder, []int{1, 2, 3, 4, 5, 6, 7, 8})

	collection, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, collection, newDataObjectFilename)
	failError(t, err)
	assert.NotEmpty(t, obj.ID)

	// delete
	err = fs.DeleteDataObject(conn, newDataObjectPath, true)
	failError(t, err)

	_, err = fs.GetDataObject(conn, collection, newDataObjectFilename)
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			deleted = true
		}
	}

	assert.True(t, deleted)
}
