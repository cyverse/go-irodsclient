package testcases

import (
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/stretchr/testify/assert"
)

func getLowlevelFileTransferTest() Test {
	return Test{
		Name: "Lowlevel_FileTransfer",
		Func: lowlevelFileTransferTest,
	}
}

func lowlevelFileTransferTest(t *testing.T, test *Test) {
	t.Run("Upload", testUpload)
	t.Run("ParallelUploadAndDownload", testParallelUploadAndDownload)
	t.Run("ParallelUploadAndDownloadWithConnections", testParallelUploadAndDownloadWithConnections)
}

func testUpload(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection(true)
	FailError(t, err)

	homeDir := test.GetTestHomeDir()

	// gen large file
	filename := "test_large_file.bin"
	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := CreateLocalTestFile(t, filename, fileSize)
	FailError(t, err)
	defer func() {
		err = os.Remove(localPath)
		FailError(t, err)
	}()

	localHash, err := util.HashLocalFile(localPath, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	// upload
	irodsPath := homeDir + "/" + filename

	transferCurrent := int64(0)
	transferTotal := int64(0)
	transferCallBack := func(current int64, total int64) {
		transferCurrent = current
		transferTotal = total
	}

	err = fs.UploadDataObject(sess, localPath, irodsPath, "", false, nil, transferCallBack)
	FailError(t, err)
	assert.Equal(t, fileSize, transferCurrent)
	assert.Equal(t, fileSize, transferTotal)

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, fileSize, obj.Size)

	objChecksum, err := fs.GetDataObjectChecksum(conn, irodsPath, "")
	FailError(t, err)

	assert.NotEmpty(t, objChecksum.Checksum)
	assert.Equal(t, types.ChecksumAlgorithmSHA256, objChecksum.Algorithm)
	assert.Equal(t, localHash, objChecksum.Checksum)

	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)

	sess.ReturnConnection(conn)
}

func testParallelUploadAndDownload(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer session.ReturnConnection(conn)

	homeDir := test.GetTestHomeDir()

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := int64(300 * 1024 * 1024) // 300MB

	localPath, err := CreateLocalTestFile(t, filename, fileSize)
	FailError(t, err)
	defer func() {
		err = os.Remove(localPath)
		FailError(t, err)
	}()

	// upload
	irodsPath := homeDir + "/" + filename

	transferCurrent := int64(0)
	transferTotal := int64(0)
	transferCallBack := func(current int64, total int64) {
		transferCurrent = current
		transferTotal = total
	}

	err = fs.UploadDataObjectParallel(session, localPath, irodsPath, "", 4, false, nil, transferCallBack)
	FailError(t, err)

	assert.Equal(t, fileSize, transferCurrent)
	assert.Equal(t, fileSize, transferTotal)

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, fileSize, obj.Size)

	// get
	transferCurrent = int64(0)
	transferTotal = int64(0)

	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	err = fs.DownloadDataObjectParallel(session, obj, "", newLocalPath, 4, nil, transferCallBack)
	FailError(t, err)

	st, err := os.Stat(newLocalPath)
	FailError(t, err)
	assert.Equal(t, fileSize, st.Size())
	assert.Equal(t, fileSize, transferCurrent)
	assert.Equal(t, fileSize, transferTotal)

	err = os.Remove(newLocalPath)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)
}

func testParallelUploadAndDownloadWithConnections(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer session.ReturnConnection(conn)

	homeDir := test.GetTestHomeDir()

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := int64(300 * 1024 * 1024) // 300MB

	localPath, err := CreateLocalTestFile(t, filename, fileSize)
	FailError(t, err)
	defer func() {
		err = os.Remove(localPath)
		FailError(t, err)
	}()

	// upload
	irodsPath := homeDir + "/" + filename

	transferCurrent := int64(0)
	transferTotal := int64(0)
	transferCallBack := func(current int64, total int64) {
		transferCurrent = current
		transferTotal = total
	}

	conns, err := session.AcquireConnectionsMulti(5, false)
	defer session.ReturnConnectionsMulti(conns)

	err = fs.UploadDataObjectParallelWithConnections(conns, localPath, irodsPath, "", false, nil, transferCallBack)
	FailError(t, err)

	assert.Equal(t, fileSize, transferCurrent)
	assert.Equal(t, fileSize, transferTotal)

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, fileSize, obj.Size)

	// get
	transferCurrent = int64(0)
	transferTotal = int64(0)

	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	err = fs.DownloadDataObjectParallelWithConnections(conns, obj, "", newLocalPath, nil, transferCallBack)
	FailError(t, err)

	st, err := os.Stat(newLocalPath)
	FailError(t, err)
	assert.Equal(t, fileSize, st.Size())
	assert.Equal(t, fileSize, transferCurrent)
	assert.Equal(t, fileSize, transferTotal)

	err = os.Remove(newLocalPath)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)
}
