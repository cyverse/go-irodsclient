package testcases

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getHighlevelFilesystemCacheConsistencyTest() Test {
	return Test{
		Name: "Highlevel_FilesystemCacheConsistency",
		Func: highlevelFilesystemCacheConsistencyTest,
	}
}

func highlevelFilesystemCacheConsistencyTest(t *testing.T, test *Test) {
	t.Run("CrossSessionCacheInconsistency_Create", testCrossSessionCacheInconsistencyCreate)
	t.Run("CrossSessionCacheInconsistency_Rename", testCrossSessionCacheInconsistencyRename)
	t.Run("CrossSessionCacheInconsistency_Delete", testCrossSessionCacheInconsistencyDelete)
	t.Run("CrossSessionCacheInconsistency_CreateAndListParent", testCrossSessionCacheInconsistencyCreateAndListParent)
	t.Run("FreshRead_Create", testFreshReadCreate)
	t.Run("FreshRead_Rename", testFreshReadRename)
	t.Run("FreshRead_Delete", testFreshReadDelete)
	t.Run("FreshRead_CreateAndListParent", testFreshReadCreateAndListParent)
}

// testCrossSessionCacheInconsistencyCreate tests that a newly created child
// is immediately visible when checking existence in a new filesystem instance
func testCrossSessionCacheInconsistencyCreate(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	testPath := fmt.Sprintf("%s/cache_test_create", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(testPath, true, true)

	// Create in filesystem1
	err = filesystem1.MakeDir(testPath, false)
	FailError(t, err)

	// Check if exists in filesystem2 (should be true but might be false due to cache)
	exists := filesystem2.ExistsDir(testPath)
	assert.True(t, exists, "directory created in fs1 should be visible immediately in fs2")

	// Cleanup
	_ = filesystem1.RemoveDir(testPath, true, true)
}

// testCrossSessionCacheInconsistencyRename tests that a renamed path
// is immediately visible with the new name in a new filesystem instance
func testCrossSessionCacheInconsistencyRename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	oldPath := fmt.Sprintf("%s/cache_test_rename_old", homeDir)
	newPath := fmt.Sprintf("%s/cache_test_rename_new", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(oldPath, true, true)
	_ = filesystem1.RemoveDir(newPath, true, true)

	// Create and rename in filesystem1
	err = filesystem1.MakeDir(oldPath, false)
	FailError(t, err)

	err = filesystem1.RenameDirToDir(oldPath, newPath)
	FailError(t, err)

	// Check if new path exists in filesystem2
	existsNew := filesystem2.ExistsDir(newPath)
	assert.True(t, existsNew, "renamed path should be visible in fs2 immediately")

	// Check if old path doesn't exist in filesystem2
	existsOld := filesystem2.ExistsDir(oldPath)
	assert.False(t, existsOld, "old path should not be visible in fs2 after rename")

	// Cleanup
	_ = filesystem1.RemoveDir(newPath, true, true)
}

// testCrossSessionCacheInconsistencyDelete tests that a deleted path
// is immediately seen as not existing in a new filesystem instance
func testCrossSessionCacheInconsistencyDelete(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	testPath := fmt.Sprintf("%s/cache_test_delete", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(testPath, true, true)

	// Create in filesystem1
	err = filesystem1.MakeDir(testPath, false)
	FailError(t, err)

	// Verify it exists
	exists := filesystem1.ExistsDir(testPath)
	assert.True(t, exists, "created directory should exist in fs1")

	// Delete in filesystem1
	err = filesystem1.RemoveDir(testPath, true, true)
	FailError(t, err)

	// Check if it doesn't exist in filesystem2
	existsAfterDelete := filesystem2.ExistsDir(testPath)
	assert.False(t, existsAfterDelete, "deleted directory should not be visible in fs2")
}

// testCrossSessionCacheInconsistencyCreateAndListParent tests that a newly created child
// appears immediately in a parent listing from a new filesystem instance
func testCrossSessionCacheInconsistencyCreateAndListParent(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	parentPath := fmt.Sprintf("%s/cache_test_parent", homeDir)
	childPath := fmt.Sprintf("%s/cache_test_parent/cache_test_child", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(parentPath, true, true)

	// Create parent in filesystem1
	err = filesystem1.MakeDir(parentPath, false)
	FailError(t, err)

	// Create child in filesystem1
	err = filesystem1.MakeDir(childPath, false)
	FailError(t, err)

	// List parent in filesystem2 (should include child)
	entries, err := filesystem2.List(parentPath)
	FailError(t, err)

	found := false
	for _, entry := range entries {
		if entry.Name == "cache_test_child" {
			found = true
			break
		}
	}

	assert.True(t, found, "newly created child should appear in fs2 parent listing")

	// Cleanup
	_ = filesystem1.RemoveDir(parentPath, true, true)
}

// testFreshReadCreate tests that StatFresh/ExistsFresh correctly show newly created paths
func testFreshReadCreate(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	testPath := fmt.Sprintf("%s/fresh_read_test_create", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(testPath, true, true)

	// Create in filesystem1
	err = filesystem1.MakeDir(testPath, false)
	FailError(t, err)

	// Use StatFresh in filesystem2 (should always see fresh state)
	entry, err := filesystem2.StatFresh(testPath)
	FailError(t, err)
	assert.NotNil(t, entry, "StatFresh should find newly created directory")

	// Check with ExistsFresh
	exists := filesystem2.ExistsFresh(testPath)
	assert.True(t, exists, "ExistsFresh should confirm newly created directory exists")

	// Check with ExistsDirFresh
	existsDir := filesystem2.ExistsDirFresh(testPath)
	assert.True(t, existsDir, "ExistsDirFresh should confirm newly created directory is a directory")

	// Cleanup
	_ = filesystem1.RemoveDir(testPath, true, true)
}

// testFreshReadRename tests that fresh-read methods show renamed paths correctly
func testFreshReadRename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	oldPath := fmt.Sprintf("%s/fresh_read_test_rename_old", homeDir)
	newPath := fmt.Sprintf("%s/fresh_read_test_rename_new", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(oldPath, true, true)
	_ = filesystem1.RemoveDir(newPath, true, true)

	// Create and rename in filesystem1
	err = filesystem1.MakeDir(oldPath, false)
	FailError(t, err)

	err = filesystem1.RenameDirToDir(oldPath, newPath)
	FailError(t, err)

	// Use StatFresh on new path in filesystem2
	entryNew, err := filesystem2.StatFresh(newPath)
	FailError(t, err)
	assert.NotNil(t, entryNew, "StatFresh should find renamed directory at new path")

	// Check old path with StatFresh
	_, errOld := filesystem2.StatFresh(oldPath)
	assert.Error(t, errOld, "StatFresh should not find directory at old path after rename")

	// Check with ExistsFresh
	existsNew := filesystem2.ExistsFresh(newPath)
	assert.True(t, existsNew, "ExistsFresh should confirm renamed directory exists at new path")

	existsOld := filesystem2.ExistsFresh(oldPath)
	assert.False(t, existsOld, "ExistsFresh should not find old path after rename")

	// Cleanup
	_ = filesystem1.RemoveDir(newPath, true, true)
}

// testFreshReadDelete tests that fresh-read methods show deleted paths as non-existent
func testFreshReadDelete(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	testPath := fmt.Sprintf("%s/fresh_read_test_delete", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(testPath, true, true)

	// Create in filesystem1
	err = filesystem1.MakeDir(testPath, false)
	FailError(t, err)

	// Delete in filesystem1
	err = filesystem1.RemoveDir(testPath, true, true)
	FailError(t, err)

	// Use StatFresh in filesystem2 (should not find deleted path)
	_, err = filesystem2.StatFresh(testPath)
	assert.Error(t, err, "StatFresh should not find deleted directory")

	// Check with ExistsFresh
	exists := filesystem2.ExistsFresh(testPath)
	assert.False(t, exists, "ExistsFresh should not find deleted directory")

	existsDir := filesystem2.ExistsDirFresh(testPath)
	assert.False(t, existsDir, "ExistsDirFresh should not find deleted directory")
}

// testFreshReadCreateAndListParent tests that ListFresh shows newly created children
func testFreshReadCreateAndListParent(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem1, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem1.Release()

	filesystem2, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem2.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	parentPath := fmt.Sprintf("%s/fresh_read_test_parent", homeDir)
	childPath := fmt.Sprintf("%s/fresh_read_test_parent/fresh_read_test_child", homeDir)

	// Delete if exists
	_ = filesystem1.RemoveDir(parentPath, true, true)

	// Create parent in filesystem1
	err = filesystem1.MakeDir(parentPath, false)
	FailError(t, err)

	// Create child in filesystem1
	err = filesystem1.MakeDir(childPath, false)
	FailError(t, err)

	// Use ListFresh in filesystem2 (should include newly created child)
	entries, err := filesystem2.ListFresh(parentPath)
	FailError(t, err)

	found := false
	for _, entry := range entries {
		if entry.Name == "fresh_read_test_child" {
			found = true
			break
		}
	}

	assert.True(t, found, "ListFresh should include child created in fs1")

	// Cleanup
	_ = filesystem1.RemoveDir(parentPath, true, true)
}
