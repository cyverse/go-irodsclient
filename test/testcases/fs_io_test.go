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
	"github.com/stretchr/testify/assert"
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

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	assert.NoError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		err = filesystem.UploadFile(localPath, iRODSPath, "", false, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		assert.NoError(t, err)

		start = time.Now()
		err = filesystem.DownloadFile(iRODSPath, "", localDownloadPath, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		assert.NoError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		assert.NoError(t, err)

		err = os.Remove(localDownloadPath)
		assert.NoError(t, err)
	}

	err = os.Remove(localPath)
	assert.NoError(t, err)
}
