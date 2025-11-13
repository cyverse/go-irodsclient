package testcases

import (
	"fmt"
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	test_server "github.com/cyverse/go-irodsclient/test/server"
	"github.com/stretchr/testify/assert"
)

func getHighlevelFileTransferTest() Test {
	return Test{
		Name: "Highlevel_FileTransfer",
		Func: highlevelFileTransferTest,
	}
}

func highlevelFileTransferTest(t *testing.T, test *Test) {
	t.Run("UploadAndDownload", testUploadAndDownload)
	t.Run("UploadAndDownloadOverwrite", testUploadAndDownloadOverwrite)
	t.Run("UploadAndDownloadParallel", testUploadAndDownloadParallel)
	t.Run("UploadAndDownloadParallelOverwrite", testUploadAndDownloadParallelOverwrite)
	t.Run("UploadAndDownloadRedirectToResource", testUploadAndDownloadRedirectToResource)
	t.Run("UploadAndDownloadRedirectToResourceOverwrite", testUploadAndDownloadRedirectToResourceOverwrite)
	t.Run("UploadAndDownload1000sRedirectToResource", testUploadAndDownload1000sRedirectToResource)
}

func testUploadAndDownload(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFile(irodsPath, "", newLocalPath, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i < 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFile(irodsPath, "", newLocalPath, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownloadParallel(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileParallel(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadParallelOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i < 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileParallel(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownloadRedirectToResource(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadRedirectToResourceOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i < 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownload1000sRedirectToResource(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1000 * 1000 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, false, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name)
		assert.Equal(t, int64(fileSize), entry.Size)
		assert.Equal(t, fs.FileEntry, entry.Type)

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if test.currentVersion == test_server.IRODS_4_2_8 {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}
