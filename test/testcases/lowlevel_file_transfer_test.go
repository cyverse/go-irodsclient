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
	server := test.GetCurrentServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection(true)
	FailError(t, err)

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	// gen large file
	filename := "test_large_file.bin"
	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := CreateLocalTestFile(t, filename, fileSize)
	FailError(t, err)
	defer func() {
		err = os.Remove(localPath)
		FailError(t, err)
	}()

	localHash, err := util.HashLocalFile(localPath, string(types.ChecksumAlgorithmSHA256), nil)
	FailError(t, err)

	// upload
	irodsPath := homeDir + "/" + filename

	transferCurrent := int64(0)
	transferTotal := int64(0)

	transferCallBack := func(taskName string, current int64, total int64) {
		if taskName == "upload" || taskName == "download" {
			transferCurrent = current
			transferTotal = total
		}
	}

	err = fs.UploadDataObject(sess, localPath, irodsPath, "", false, nil, transferCallBack)
	FailError(t, err)
	assert.Equal(t, fileSize, transferCurrent, "upload progress callback should report full file bytes transferred")
	assert.Equal(t, fileSize, transferTotal, "upload progress callback should report full total file bytes")

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID, "object ID should not be empty")
	assert.Equal(t, fileSize, obj.Size, "object size should match uploaded file size")

	objChecksum, err := fs.GetDataObjectChecksum(conn, irodsPath, "")
	FailError(t, err)

	assert.NotEmpty(t, objChecksum.Checksum, "uploaded object should have computed checksum")
	assert.Equal(t, types.ChecksumAlgorithmSHA256, objChecksum.Algorithm, "object checksum should use SHA256 algorithm")
	assert.Equal(t, localHash, objChecksum.Checksum, "object checksum should match source file hash")

	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)

	_ = sess.ReturnConnection(conn)
}

func testParallelUploadAndDownload(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

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

	transferCallBack := func(taskName string, current int64, total int64) {
		if taskName == "upload" || taskName == "download" {
			transferCurrent = current
			transferTotal = total
		}
	}

	err = fs.UploadDataObjectParallel(session, localPath, irodsPath, "", 4, false, nil, transferCallBack)
	FailError(t, err)

	assert.Equal(t, fileSize, transferCurrent, "parallel upload progress should report full file bytes")
	assert.Equal(t, fileSize, transferTotal, "parallel upload progress should report full total size")

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID, "object ID should not be empty")
	assert.Equal(t, fileSize, obj.Size, "object size should match uploaded file size")

	// get
	transferCurrent = int64(0)
	transferTotal = int64(0)

	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	err = fs.DownloadDataObjectParallel(session, obj, "", newLocalPath, 4, nil, transferCallBack)
	FailError(t, err)

	st, err := os.Stat(newLocalPath)
	FailError(t, err)
	assert.Equal(t, fileSize, st.Size(), "downloaded file size should match iRODS object size")
	assert.Equal(t, fileSize, transferCurrent, "parallel download progress should report full file bytes")
	assert.Equal(t, fileSize, transferTotal, "parallel download progress should report full total size")

	err = os.Remove(newLocalPath)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)
}

func testParallelUploadAndDownloadWithConnections(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

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

	transferCallBack := func(taskName string, current int64, total int64) {
		if taskName == "upload" || taskName == "download" {
			transferCurrent = current
			transferTotal = total
		}
	}

	conns, err := session.AcquireConnectionsMulti(5, false)
	defer func() {
		_ = session.ReturnConnectionsMulti(conns)
	}()

	err = fs.UploadDataObjectParallelWithConnections(conns, localPath, irodsPath, "", false, nil, transferCallBack)
	FailError(t, err)

	assert.Equal(t, fileSize, transferCurrent, "parallel upload with connections should report full bytes transferred")
	assert.Equal(t, fileSize, transferTotal, "parallel upload with connections should report full total size")

	obj, err := fs.GetDataObject(conn, irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID, "uploaded object via connections should have assigned ID")
	assert.Equal(t, fileSize, obj.Size, "uploaded object via connections size should match source")

	// get
	transferCurrent = int64(0)
	transferTotal = int64(0)

	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	err = fs.DownloadDataObjectParallelWithConnections(conns, obj, "", newLocalPath, nil, transferCallBack)
	FailError(t, err)

	st, err := os.Stat(newLocalPath)
	FailError(t, err)
	assert.Equal(t, fileSize, st.Size(), "downloaded file via connections should match object size")
	assert.Equal(t, fileSize, transferCurrent, "parallel download with connections should report full bytes")
	assert.Equal(t, fileSize, transferTotal, "parallel download with connections should report full total")

	err = os.Remove(newLocalPath)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)
}
