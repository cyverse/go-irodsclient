package testcases

import (
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	redirectionToResourceAPITestID = xid.New().String()
)

func TestRedirectionToResourceAPI(t *testing.T) {
	setup()
	defer shutdown()

	log.SetLevel(log.DebugLevel)

	makeHomeDir(t, redirectionToResourceAPITestID)

	t.Run("test DownloadDataObjectFromResourceServer", testDownloadDataObjectFromResourceServer)
}

func testDownloadDataObjectFromResourceServer(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSessionWithAddressResolver(account, sessionConfig, server.AddressResolver)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(redirectionToResourceAPITestID)

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

	checksumOriginal, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA1))
	failError(t, err)

	err = os.Remove(filename)
	failError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	err = fs.DownloadDataObjectFromResourceServer(sess, irodsPath, "", filename, int64(fileSize), callBack)
	failError(t, err)

	checksumNew, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA1))
	failError(t, err)

	err = os.Remove(filename)
	failError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	assert.Equal(t, checksumOriginal, checksumNew)

	sess.ReturnConnection(conn)
}
