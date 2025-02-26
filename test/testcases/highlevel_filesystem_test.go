package testcases

import (
	"fmt"
	"path"
	"testing"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/stretchr/testify/assert"
)

func getHighlevelFilesystemTest() Test {
	return Test{
		Name: "Highlevel_Filesystem",
		Func: highlevelFilesystemTest,
	}
}

func highlevelFilesystemTest(t *testing.T, test *Test) {
	t.Run("MakeDir", testMakeDir)
	t.Run("MakeDirRecurse", testMakeDirRecurse)
	t.Run("UploadAndDeleteDir", testUploadAndDeleteDir)
}

func testMakeDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFilesystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	for i := 0; i < 10; i++ {
		newDir := fmt.Sprintf("%s/test_dir_%d", homeDir, i)

		// create test
		err = filesystem.MakeDir(newDir, false)
		FailError(t, err)

		entries, err := filesystem.List(homeDir)
		FailError(t, err)

		found := false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newDir {
				// okay
				found = true
				break
			}
		}

		assert.True(t, found)

		exist := filesystem.ExistsDir(newDir)
		assert.True(t, exist)

		// delete test
		err = filesystem.RemoveDir(newDir, true, true)
		FailError(t, err)

		entries, err = filesystem.List(homeDir)
		FailError(t, err)

		found = false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newDir {
				// found removed dir?
				found = true
				break
			}
		}

		assert.False(t, found)
	}
}

func testMakeDirRecurse(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFilesystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	newDir := fmt.Sprintf("%s/make_dir_recurse", homeDir)

	// get side connection
	conn, err := filesystem.GetMetadataConnection()
	FailError(t, err)
	defer filesystem.ReturnMetadataConnection(conn)

	// stat first
	dirStat, err := filesystem.StatDir(newDir)
	assert.Nil(t, dirStat)
	assert.Error(t, err)

	// make dir using the side connection without cache update
	err = irods_fs.CreateCollection(conn, newDir, false)
	FailError(t, err)

	// make dir using the side connection without cache update - again
	err = irods_fs.CreateCollection(conn, newDir, true)
	FailError(t, err)

	// make dir
	err = filesystem.MakeDir(newDir, true)
	FailError(t, err)

	dirStat, err = filesystem.StatDir(newDir)
	FailError(t, err)

	assert.NotNil(t, dirStat)
	assert.Equal(t, newDir, dirStat.Path)
	assert.True(t, dirStat.IsDir())

	// remove
	err = filesystem.RemoveDir(newDir, true, true)
	FailError(t, err)
}

func testUploadAndDeleteDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFilesystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := CreateLocalTestFile(t, "test_file_", fileSize)
	FailError(t, err)

	for i := 0; i < 10; i++ {
		newDir := fmt.Sprintf("%s/test_dir_%d", homeDir, i)

		// create test
		err = filesystem.MakeDir(newDir, false)
		FailError(t, err)

		exist := filesystem.ExistsDir(newDir)
		assert.True(t, exist)

		// upload
		iRODSPath := fmt.Sprintf("%s/%s", newDir, path.Base(localPath))
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, false, nil)
		FailError(t, err)

		// delete dir recursively
		err = filesystem.RemoveDir(newDir, true, true)
		FailError(t, err)

		exist = filesystem.ExistsDir(newDir)
		assert.False(t, exist)
	}
}
