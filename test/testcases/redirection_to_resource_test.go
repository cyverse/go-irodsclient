package testcases

import (
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
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
	t.Run("test UploadDataObjectFromResourceServer", testUploadDataObjectFromResourceServer)
}

func testDownloadDataObjectFromResourceServer(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(redirectionToResourceAPITestID)

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := 100 * 1024 * 1024 // 100MB

	filepath, err := createLocalTestFile(filename, int64(fileSize))
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectParallel(sess, filepath, irodsPath, "", 4, false, nil, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 3) // at least called 3 times

	checksumOriginal, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	failError(t, err)

	err = os.Remove(filepath)
	failError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	keywords := map[common.KeyWord]string{
		common.VERIFY_CHKSUM_KW: "",
	}

	checksum, err := fs.DownloadDataObjectFromResourceServer(sess, irodsPath, "", filename, int64(fileSize), 0, keywords, callBack)
	failError(t, err)

	assert.NotEmpty(t, checksum)

	checksumNew, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA256))
	failError(t, err)

	err = os.Remove(filename)
	failError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	assert.Equal(t, checksumOriginal, checksumNew)

	checksumAlg, checksumStr, err := types.ParseIRODSChecksumString(checksum)
	failError(t, err)
	assert.Equal(t, checksumAlg, types.ChecksumAlgorithmSHA256)
	assert.Equal(t, checksumNew, checksumStr)

	sess.ReturnConnection(conn)
}

func testUploadDataObjectFromResourceServer(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(redirectionToResourceAPITestID)

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := 100 * 1024 * 1024 // 100MB

	filepath, err := createLocalTestFile(filename, int64(fileSize))
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectToResourceServer(sess, filepath, irodsPath, "", 0, false, nil, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 3) // at least called 3 times

	checksumOriginal, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	failError(t, err)

	err = os.Remove(filepath)
	failError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	keywords := map[common.KeyWord]string{
		common.VERIFY_CHKSUM_KW: "",
	}

	checksum, err := fs.DownloadDataObjectFromResourceServer(sess, irodsPath, "", filename, int64(fileSize), 0, keywords, callBack)
	failError(t, err)

	assert.NotEmpty(t, checksum)

	checksumNew, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA256))
	failError(t, err)

	err = os.Remove(filename)
	failError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	assert.Equal(t, checksumOriginal, checksumNew)

	checksumAlg, checksumStr, err := types.ParseIRODSChecksumString(checksum)
	failError(t, err)
	assert.Equal(t, checksumAlg, types.ChecksumAlgorithmSHA256)
	assert.Equal(t, checksumNew, checksumStr)

	sess.ReturnConnection(conn)
}
