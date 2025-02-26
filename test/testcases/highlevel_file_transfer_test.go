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
	t.Run("UploadAndDownloadRedirectToResource", testUploadAndDownloadRedirectToResource)

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

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, true, false, nil)
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

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, true, false, nil)
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

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, true, false, nil)
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

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, true, false, nil)
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

/*
func testUpRemoveMBFiles(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)
	testDirPath := path.Join(homedir, "test_dir")

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	FailError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", testDirPath, path.Base(localPath))

	// core
	for i := 0; i < 100; i++ {
		t.Logf("iteration %d, making dir", i)
		err = filesystem.MakeDir(testDirPath, true)
		FailError(t, err)

		t.Logf("iteration %d, uploading file", i)
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, false, nil)
		FailError(t, err)

		t.Logf("iteration %d, stating file", i)
		fileStat, err := filesystem.Stat(iRODSPath)
		FailError(t, err)

		if fileStat.Size != fileSize {
			FailError(t, fmt.Errorf("wrong size"))
		}

		t.Logf("iteration %d, removing dir", i)
		err = filesystem.RemoveDir(testDirPath, true, true)
		FailError(t, err)

		t.Logf("iteration %d, stating file again", i)
		fileStat2, err := filesystem.Stat(iRODSPath)
		if err != nil {
			if !types.IsFileNotFoundError(err) {
				FailError(t, err)
			}
		} else {
			FailError(t, fmt.Errorf("file not deleted - %q (size %d)", fileStat2.Path, fileStat2.Size))
		}
	}

	err = os.Remove(localPath)
	FailError(t, err)
}


*/
