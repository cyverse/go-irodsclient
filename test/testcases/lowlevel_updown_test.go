package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/stretchr/testify/assert"
)

func getLowlevelUpdownTest() Test {
	return Test{
		Name: "Lowlevel_Updown",
		Func: lowlevelUpdownTest,
	}
}

func lowlevelUpdownTest(t *testing.T, test *Test) {
	t.Run("UploadDataObject", testUploadDataObject)
}

func testUploadDataObject(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	FailError(t, err)

	homedir := test.GetTestHomeDir()

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := 100 * 1024 * 1024 // 100MB
	filepath, err := CreateLocalTestFile(t, filename, int64(fileSize))
	FailError(t, err)

	localHash, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObject(sess, filepath, irodsPath, "", false, nil, callBack)
	FailError(t, err)
	assert.Greater(t, callbackCalled, 10) // at least called 10 times

	coll, err := fs.GetCollection(conn, homedir)
	FailError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	objChecksum, err := fs.GetDataObjectChecksum(conn, irodsPath, "")
	FailError(t, err)

	assert.NotEmpty(t, objChecksum.Checksum)
	assert.Equal(t, types.ChecksumAlgorithmSHA256, objChecksum.Algorithm)
	assert.Equal(t, localHash, objChecksum.Checksum)
	//t.Logf("alg: %s, checksum: %s", objChecksum.Algorithm, objChecksum.GetChecksumString())

	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)

	sess.ReturnConnection(conn)
}
