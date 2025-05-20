package testcases

import (
	"sync"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func getLowlevelLockTest() Test {
	return Test{
		Name: "Lowlevel_Lock",
		Func: lowlevelLockTest,
	}
}

func lowlevelLockTest(t *testing.T, test *Test) {
	t.Run("LockDataObject", testLockDataObject)
}

func testLockDataObject(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn1, err := session.AcquireConnection(true)
	FailError(t, err)
	defer session.ReturnConnection(conn1)

	conn2, err := session.AcquireConnection(true)
	FailError(t, err)
	defer session.ReturnConnection(conn2)

	homeDir := test.GetTestHomeDir()

	// create
	newDataObjectFilename := "locktest.bin"
	newDataObjectPath := homeDir + "/" + newDataObjectFilename
	writeData := MakeFixedContentDataBuf(1023)

	taskOrder := []int{}
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		handle, err := fs.CreateDataObject(conn1, newDataObjectPath, "", "w", true, nil)
		FailError(t, err)

		// lock
		//t.Log("try lock 1")
		taskOrder = append(taskOrder, 1)
		lockHandle, err := fs.LockDataObject(conn1, newDataObjectPath, types.DataObjectLockTypeWrite, types.DataObjectLockCommandSetLockWait)
		FailError(t, err)
		//t.Log("locked 1")
		taskOrder = append(taskOrder, 2)

		wg.Add(1)
		go func() {
			// lock2 - must wait
			//t.Log("try lock 2")
			taskOrder = append(taskOrder, 3)
			lockHandle2, err := fs.LockDataObject(conn2, newDataObjectPath, types.DataObjectLockTypeWrite, types.DataObjectLockCommandSetLockWait)
			FailError(t, err)
			//t.Log("locked 2")
			taskOrder = append(taskOrder, 6)

			// unlock
			//t.Log("try unlock 2")
			taskOrder = append(taskOrder, 7)
			err = fs.UnlockDataObject(conn2, lockHandle2)
			FailError(t, err)
			//t.Log("unlocked 2")
			taskOrder = append(taskOrder, 8)

			wg.Done()
		}()

		// sleep 5 sec
		time.Sleep(5 * time.Second)

		err = fs.WriteDataObject(conn1, handle, writeData)
		FailError(t, err)

		// unlock
		//t.Log("try unlock 1")
		taskOrder = append(taskOrder, 4)
		err = fs.UnlockDataObject(conn1, lockHandle)
		FailError(t, err)
		//t.Log("unlocked 1")
		taskOrder = append(taskOrder, 5)

		err = fs.CloseDataObject(conn1, handle)
		FailError(t, err)

		wg.Done()
	}()

	wg.Wait()

	assert.ElementsMatch(t, taskOrder, []int{1, 2, 3, 4, 5, 6, 7, 8})

	obj, err := fs.GetDataObject(conn1, newDataObjectPath)
	FailError(t, err)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(len(writeData)), obj.Size)

	// delete
	err = fs.DeleteDataObject(conn1, newDataObjectPath, true)
	FailError(t, err)

	_, err = fs.GetDataObject(conn1, newDataObjectPath)
	deleted := false
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// Okay!
			deleted = true
		}
	}

	assert.True(t, deleted)
}
