package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/stretchr/testify/assert"
)

func getHighlevelFilesystemCacheTest() Test {
	return Test{
		Name: "Highlevel_FilesystemCache",
		Func: highlevelFilesystemCacheTest,
	}
}

func highlevelFilesystemCacheTest(t *testing.T, test *Test) {
	t.Run("MakeDirCacheEvent", testMakeDirCacheEvent)
}

func testMakeDirCacheEvent(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFilesystem()
	FailError(t, err)
	defer filesystem.Release()

	eventTypesReceived := []fs.FilesystemCacheEventType{}
	eventPathsReceived := []string{}
	eventHandler := func(path string, eventType fs.FilesystemCacheEventType) {
		eventTypesReceived = append(eventTypesReceived, eventType)
		eventPathsReceived = append(eventPathsReceived, path)
	}

	filesystem.AddCacheEventHandler(eventHandler)

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 10; i++ {
		newDir := fmt.Sprintf("%s/cache_test_dir_%d", homeDir, i)

		// create test
		err = filesystem.MakeDir(newDir, false)
		FailError(t, err)

		exist := filesystem.ExistsDir(newDir)
		assert.True(t, exist)

		// delete test
		err = filesystem.RemoveDir(newDir, true, true)
		FailError(t, err)

		assert.Equal(t, 2, len(eventTypesReceived))
		assert.Equal(t, 2, len(eventPathsReceived))

		assert.Equal(t, newDir, eventPathsReceived[0])
		assert.Equal(t, fs.FilesystemCacheDirCreateEvent, eventTypesReceived[0])
		assert.Equal(t, newDir, eventPathsReceived[1])
		assert.Equal(t, fs.FilesystemCacheDirRemoveEvent, eventTypesReceived[1])

		eventTypesReceived = []fs.FilesystemCacheEventType{}
		eventPathsReceived = []string{}
	}
}
