package testcases

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/rs/xid"
)

var (
	fsIOTestID = xid.New().String()
)

func TestFSIO(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, fsIOTestID)

	t.Run("test UpDownMBFiles", testUpDownMBFiles)
	t.Run("test UpDownMBFilesParallel", testUpDownMBFilesParallel)
	t.Run("test UpDownMBFilesParallelRedirectToResource", testUpDownMBFilesParallelRedirectToResource)
}

func testUpDownMBFiles(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	failError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	failError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFile(iRODSPath, "", localDownloadPath, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		failError(t, err)

		err = os.Remove(localDownloadPath)
		failError(t, err)
	}

	err = os.Remove(localPath)
	failError(t, err)
}

func testUpDownMBFilesParallel(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	failError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	failError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFileParallel(localPath, iRODSPath, "", 0, false, true, true, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFileParallel(iRODSPath, "", localDownloadPath, 0, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		failError(t, err)

		err = os.Remove(localDownloadPath)
		failError(t, err)
	}

	err = os.Remove(localPath)
	failError(t, err)
}

func testUpDownMBFilesParallelRedirectToResource(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	failError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	failError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFileParallelRedirectToResource(localPath, iRODSPath, "", 0, false, true, true, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFileParallelResumable(iRODSPath, "", localDownloadPath, 0, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		failError(t, err)

		err = os.Remove(localDownloadPath)
		failError(t, err)
	}

	err = os.Remove(localPath)
	failError(t, err)
}
