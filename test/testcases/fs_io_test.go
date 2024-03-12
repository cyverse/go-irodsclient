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
}

func testUpDownMBFiles(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig, nil)
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
		err = filesystem.UploadFile(localPath, iRODSPath, "", false, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		failError(t, err)

		start = time.Now()
		err = filesystem.DownloadFile(iRODSPath, "", localDownloadPath, nil)
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
