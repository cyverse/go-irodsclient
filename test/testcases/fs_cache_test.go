package testcases

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	fsCacheTestID = xid.New().String()
)

func TestFSCache(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, fsCacheTestID)

	t.Run("test MakeDir", testMakeDir)
	t.Run("test MakeDirCacheEvent", testMakeDirCacheEvent)
	t.Run("test MakeDirRecurse", testMakeDirRecurse)
	t.Run("test UploadAndDeleteDir", testUploadAndDeleteDir)
}

func testMakeDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsCacheTestID)

	for i := 0; i < 10; i++ {
		newdir := fmt.Sprintf("%s/test_dir_%d", homedir, i)

		// create test
		err = filesystem.MakeDir(newdir, false)
		failError(t, err)

		entries, err := filesystem.List(homedir)
		failError(t, err)

		found := false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newdir {
				// okay
				found = true
				break
			}
		}

		assert.True(t, found)

		exist := filesystem.ExistsDir(newdir)
		assert.True(t, exist)

		// delete test
		err = filesystem.RemoveDir(newdir, true, true)
		failError(t, err)

		entries, err = filesystem.List(homedir)
		failError(t, err)

		found = false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newdir {
				// found removed dir?
				found = true
				break
			}
		}

		assert.False(t, found)
	}
}

func testMakeDirCacheEvent(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	eventTypesReceived := []fs.FilesystemCacheEventType{}
	eventPathsReceived := []string{}
	eventHandler := func(path string, eventType fs.FilesystemCacheEventType) {
		eventTypesReceived = append(eventTypesReceived, eventType)
		eventPathsReceived = append(eventPathsReceived, path)
	}

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	filesystem.AddCacheEventHandler(eventHandler)

	homedir := getHomeDir(fsCacheTestID)

	for i := 0; i < 10; i++ {
		newdir := fmt.Sprintf("%s/cache_test_dir_%d", homedir, i)

		// create test
		err = filesystem.MakeDir(newdir, false)
		failError(t, err)

		exist := filesystem.ExistsDir(newdir)
		assert.True(t, exist)

		// delete test
		err = filesystem.RemoveDir(newdir, true, true)
		failError(t, err)

		assert.Equal(t, 2, len(eventTypesReceived))
		assert.Equal(t, 2, len(eventPathsReceived))

		assert.Equal(t, newdir, eventPathsReceived[0])
		assert.Equal(t, fs.FilesystemCacheDirCreateEvent, eventTypesReceived[0])
		assert.Equal(t, newdir, eventPathsReceived[1])
		assert.Equal(t, fs.FilesystemCacheDirRemoveEvent, eventTypesReceived[1])

		eventTypesReceived = []fs.FilesystemCacheEventType{}
		eventPathsReceived = []string{}
	}
}

func testMakeDirRecurse(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsCacheTestID)

	newdir := fmt.Sprintf("%s/make_dir_recurse", homedir)

	// get side connection
	conn, err := filesystem.GetMetadataConnection()
	failError(t, err)
	defer filesystem.ReturnMetadataConnection(conn)

	// stat first
	dirStat, err := filesystem.StatDir(newdir)
	assert.Nil(t, dirStat)
	assert.Error(t, err)

	// make dir using the side connection without cache update
	err = irods_fs.CreateCollection(conn, newdir, false)
	failError(t, err)

	// make dir using the side connection without cache update - again
	err = irods_fs.CreateCollection(conn, newdir, true)
	failError(t, err)

	// make dir
	err = filesystem.MakeDir(newdir, true)
	failError(t, err)

	dirStat, err = filesystem.StatDir(newdir)
	assert.NotNil(t, dirStat)
	failError(t, err)
	assert.Equal(t, newdir, dirStat.Path)
	assert.True(t, dirStat.IsDir())

	// remove
	err = filesystem.RemoveDir(newdir, true, true)
	failError(t, err)
}

func testUploadAndDeleteDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsCacheTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	failError(t, err)

	for i := 0; i < 10; i++ {
		newdir := fmt.Sprintf("%s/test_dir_%d", homedir, i)

		// create test
		err = filesystem.MakeDir(newdir, false)
		failError(t, err)

		exist := filesystem.ExistsDir(newdir)
		assert.True(t, exist)

		// upload
		iRODSPath := fmt.Sprintf("%s/%s", newdir, path.Base(localPath))
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, false, nil)
		failError(t, err)

		// delete test
		err = filesystem.RemoveDir(newdir, true, true)
		failError(t, err)

		exist = filesystem.ExistsDir(newdir)
		assert.False(t, exist)
	}

	err = os.Remove(localPath)
	failError(t, err)
}
