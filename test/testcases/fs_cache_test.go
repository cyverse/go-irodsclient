package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
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
	t.Run("test testMakeDirCacheEvent", testMakeDirCacheEvent)
}

func testMakeDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

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

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

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
