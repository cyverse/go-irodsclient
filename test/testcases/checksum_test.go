package testcases

import (
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
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

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(checksumAPITestID)

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := 100 * 1024 * 1024 // 100MB
	filepath, err := createLocalTestFile(filename, int64(fileSize))
	failError(t, err)

	localHash, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	failError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObject(sess, filepath, irodsPath, "", false, nil, callBack)
	failError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	// delete
	err = os.Remove(filepath)
	failError(t, err)

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

	err = fs.DeleteDataObject(conn, irodsPath, true)
	failError(t, err)

	sess.ReturnConnection(conn)
}
