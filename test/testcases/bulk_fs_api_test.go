package testcases

import (
	"os"
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
}

func testParallelUploadDataObject(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	assert.NoError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	assert.NoError(t, err)

	homedir := getHomeDir(bulkFSAPITestID)

	// gen very large file
	testval := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62
	fileSize := 100 * 1024 * 1024                                               // 100MB

	filename := "test_large_file.bin"
	bufSize := 1024
	buf := make([]byte, bufSize)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	assert.NoError(t, err)

	for i := 0; i < fileSize/bufSize; i++ {
		// fill buf
		for j := 0; j < bufSize; j++ {
			buf[j] = testval[j%len(testval)]
		}

		_, err = f.Write(buf)
		assert.NoError(t, err)
	}

	err = f.Close()
	assert.NoError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectParallel(sess, filename, irodsPath, "", 4, false, callBack)
	assert.NoError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	err = os.Remove(filename)
	assert.NoError(t, err)

	// get
	callbackCalled = 0
	err = fs.DownloadDataObjectParallel(sess, irodsPath, "", filename, int64(fileSize), 4, callBack)
	assert.NoError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	err = os.Remove(filename)
	assert.NoError(t, err)

	collection, err := fs.GetCollection(conn, homedir)
	assert.NoError(t, err)

	obj, err := fs.GetDataObject(conn, collection, filename)
	assert.NoError(t, err)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	assert.NoError(t, err)

	sess.ReturnConnection(conn)
}
