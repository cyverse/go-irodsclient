package testcases

import (
	"crypto/sha256"
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	checksumAPITestID = xid.New().String()
)

func TestChecksumAPI(t *testing.T) {
	setup()
	defer shutdown()

	log.SetLevel(log.DebugLevel)

	makeHomeDir(t, checksumAPITestID)

	t.Run("test Checksum", testChecksum)
}

func testChecksum(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(checksumAPITestID)

	// gen very large file
	testval := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62
	fileSize := 100 * 1024 * 1024                                               // 100MB

	filename := "test_large_file.bin"
	bufSize := 1024
	buf := make([]byte, bufSize)

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	failError(t, err)

	hasher := sha256.New()

	for i := 0; i < fileSize/bufSize; i++ {
		// fill buf
		for j := 0; j < bufSize; j++ {
			buf[j] = testval[j%len(testval)]
		}

		_, err = f.Write(buf)
		hasher.Write(buf)
		failError(t, err)
	}

	err = f.Close()
	localHash := hasher.Sum(nil)
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObject(sess, filename, irodsPath, "", false, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	coll, err := fs.GetCollection(conn, homedir)
	failError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	failError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	objChecksum, err := fs.GetDataObjectChecksum(conn, irodsPath, "")
	failError(t, err)

	assert.NotEmpty(t, objChecksum.Checksum)
	assert.Equal(t, types.ChecksumAlgorithmSHA256, objChecksum.Algorithm)
	assert.Equal(t, localHash, objChecksum.Checksum)
	//t.Logf("alg: %s, checksum: %s", objChecksum.Algorithm, objChecksum.GetChecksumString())

	// delete
	err = os.Remove(filename)
	failError(t, err)

	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	sess.ReturnConnection(conn)
}
