package testcases

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	bulkFSAPITestID = xid.New().String()
)

func TestBulkFSAPI(t *testing.T) {
	setup()
	defer shutdown()

	log.SetLevel(log.DebugLevel)

	makeHomeDir(t, bulkFSAPITestID)

	t.Run("test ParallelUploadDataObject", testParallelUploadDataObject)
	t.Run("test ParallelUploadReplication", testParallelUploadReplication)
	t.Run("test ParallelUploadReplicationMulti", testParallelUploadReplicationMulti)
}

func testParallelUploadDataObject(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(bulkFSAPITestID)

	// gen very large file
	testval := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62
	fileSize := 100 * 1024 * 1024                                               // 100MB

	filename := "test_large_file.bin"
	bufSize := 1024
	buf := make([]byte, bufSize)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	failError(t, err)

	for i := 0; i < fileSize/bufSize; i++ {
		// fill buf
		for j := 0; j < bufSize; j++ {
			buf[j] = testval[j%len(testval)]
		}

		_, err = f.Write(buf)
		failError(t, err)
	}

	err = f.Close()
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectParallel(sess, filename, irodsPath, "", 4, false, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	err = os.Remove(filename)
	failError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	callbackCalled = 0
	err = fs.DownloadDataObjectParallel(sess, irodsPath, "", filename, int64(fileSize), 4, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	err = os.Remove(filename)
	failError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	sess.ReturnConnection(conn)
}

func parallelUploadReplication(t *testing.T, sess *session.IRODSSession, filename string) {
	homedir := getHomeDir(bulkFSAPITestID)

	// gen a large file, 50MB is enough
	testval := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62
	fileSize := 50 * 1024 * 1024                                                // 50MB

	bufSize := 1024
	buf := make([]byte, bufSize)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	failError(t, err)

	for i := 0; i < fileSize/bufSize; i++ {
		// fill buf
		for j := 0; j < bufSize; j++ {
			buf[j] = testval[j%len(testval)]
		}

		_, err = f.Write(buf)
		failError(t, err)
	}

	err = f.Close()
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectParallel(sess, filename, irodsPath, "replResc", 4, false, callBack)
	failError(t, err)

	err = os.Remove(filename)
	failError(t, err)

	newConn, err := sess.AcquireConnection()
	failError(t, err)

	coll, err := fs.GetCollection(newConn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(newConn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)
	if obj.Size != int64(fileSize) {
		t.Logf("error file - %s", irodsPath)
		t.FailNow()
	}
	assert.Equal(t, 2, len(obj.Replicas))

	assert.Equal(t, obj.Replicas[0].CheckSum, obj.Replicas[1].CheckSum)
	assert.Equal(t, obj.Replicas[0].Status, obj.Replicas[1].Status)

	// delete
	err = fs.DeleteDataObject(newConn, irodsPath, true)
	failError(t, err)

	sess.ReturnConnection(newConn)
}

func testParallelUploadReplicationMulti(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)

	filenamePattern := "test_repl_file_%d.bin"

	for repeat := 0; repeat < 10; repeat++ {
		wg := sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			wg.Add(1)
			filename := fmt.Sprintf(filenamePattern, i)

			go func(sess *session.IRODSSession, filename string) {
				parallelUploadReplication(t, sess, filename)
				wg.Done()
			}(sess, filename)
		}

		wg.Wait()
	}

	sess.Release()
}

func testParallelUploadReplication(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)

	parallelUploadReplication(t, sess, "test_repl_file.bin")

	sess.Release()
}
