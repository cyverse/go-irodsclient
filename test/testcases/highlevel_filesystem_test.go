package testcases

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/fs"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
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
	t.Run("ListDirectory", testListDirectory)
	t.Run("SearchByMeta", testSearchByMeta)
	t.Run("ListACLs", testListACLs)
	t.Run("CreateStat", testCreateStat)
	t.Run("SpecialCharInFilename", testSpecialCharInFilename)
	t.Run("WriteRename", testWriteRename)
	t.Run("WriteRenameDir", testWriteRenameDir)
	t.Run("RemoveClose", testRemoveClose)
}

func testMakeDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 0; i < 10; i++ {
		newDir := fmt.Sprintf("%s/test_dir_%d", homeDir, i)

		// create test
		err = filesystem.MakeDir(newDir, false)
		FailError(t, err)

		entries, err := filesystem.List(homeDir)
		FailError(t, err)

		found := false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID, "directory listing entry should have assigned iRODS ID")
			if entry.Path == newDir {
				// okay
				found = true
				break
			}
		}

		assert.True(t, found, "created directory should be found in listing")

		exist := filesystem.ExistsDir(newDir)
		assert.True(t, exist, "created directory should exist")

		// delete test
		err = filesystem.RemoveDir(newDir, true, true)
		FailError(t, err)

		entries, err = filesystem.List(homeDir)
		FailError(t, err)

		found = false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID, "directory listing entry should have assigned iRODS ID")
			if entry.Path == newDir {
				// found removed dir?
				found = true
				break
			}
		}

		assert.False(t, found, "deleted directory should not be in listing")
	}
}

func testMakeDirRecurse(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	newDir := fmt.Sprintf("%s/make_dir_recurse", homeDir)

	// get side connection
	conn, err := filesystem.GetMetadataConnection(true)
	FailError(t, err)
	defer func() {
		_ = filesystem.ReturnMetadataConnection(conn)
	}()

	// stat first
	dirStat, err := filesystem.StatDir(newDir)
	assert.Nil(t, dirStat, "stat of non-existent directory should return nil")
	assert.Error(t, err, "stat of non-existent directory should error")

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

	assert.NotNil(t, dirStat, "stat of created directory should not be nil")
	assert.Equal(t, newDir, dirStat.Path, "stat path result should match requested directory path")
	assert.True(t, dirStat.IsDir(), "stat should mark created object as directory")

	// remove
	err = filesystem.RemoveDir(newDir, true, true)
	FailError(t, err)
}

func testUploadAndDeleteDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := CreateLocalTestFile(t, "test_file_", fileSize)
	FailError(t, err)
	defer func() {
		err = os.Remove(localPath)
		FailError(t, err)
	}()

	for i := 0; i < 10; i++ {
		newDir := fmt.Sprintf("%s/test_dir_%d", homeDir, i)

		// create test
		err = filesystem.MakeDir(newDir, false)
		FailError(t, err)

		exist := filesystem.ExistsDir(newDir)
		assert.True(t, exist, "ExistsDir should confirm created directory exists")

		// upload
		iRODSPath := fmt.Sprintf("%s/%s", newDir, path.Base(localPath))
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, nil)
		FailError(t, err)

		// delete dir recursively
		err = filesystem.RemoveDir(newDir, true, true)
		FailError(t, err)

		exist = filesystem.ExistsDir(newDir)
		assert.False(t, exist, "ExistsDir should confirm deleted directory no longer exists")
	}
}

func testListDirectory(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	files, dirs, err := CreateSampleFilesAndDirs(t, server, homeDir, 5, 5)
	FailError(t, err)
	defer func() {
		for _, file := range files {
			err = filesystem.RemoveFile(file, true)
			FailError(t, err)
		}

		for _, dir := range dirs {
			err = filesystem.RemoveDir(dir, true, true)
			FailError(t, err)
		}
	}()

	entries, err := filesystem.List(homeDir)
	FailError(t, err)

	numFiles := 0
	numDirs := 0
	for _, entry := range entries {
		if entry.IsDir() {
			assert.Contains(t, dirs, entry.Path, "listed directory should be in created directories")
			numDirs++
		} else {
			assert.Contains(t, files, entry.Path, "listed file should be in created files")
			numFiles++
		}
		assert.NotEmpty(t, entry.ID, "listing entry should have assigned iRODS ID")
	}

	assert.Equal(t, len(dirs), numDirs, "count of listed directories should match created count")
	assert.Equal(t, len(files), numFiles, "count of listed files should match created count")
}

func testSearchByMeta(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	// set 1
	files1, dirs1, err := CreateSampleFilesAndDirs(t, server, homeDir, 3, 3)
	FailError(t, err)
	defer func() {
		for _, file := range files1 {
			err = filesystem.RemoveFile(file, true)
			FailError(t, err)
		}

		for _, dir := range dirs1 {
			err = filesystem.RemoveDir(dir, true, true)
			FailError(t, err)
		}
	}()

	// add some meta
	for _, file := range files1 {
		err = filesystem.AddMetadata(file, "my_key", "my_value", "")
		FailError(t, err)
	}
	for _, dir := range dirs1 {
		err = filesystem.AddMetadata(dir, "my_key", "my_value", "")
		FailError(t, err)
	}

	// set 2
	files2, dirs2, err := CreateSampleFilesAndDirs(t, server, homeDir, 3, 3)
	FailError(t, err)
	defer func() {
		for _, file := range files2 {
			err = filesystem.RemoveFile(file, true)
			FailError(t, err)
		}

		for _, dir := range dirs2 {
			err = filesystem.RemoveDir(dir, true, true)
			FailError(t, err)
		}
	}()

	// add some meta
	for _, file := range files2 {
		err = filesystem.AddMetadata(file, "my_key", "my_new_value", "")
		FailError(t, err)
	}
	for _, dir := range dirs2 {
		err = filesystem.AddMetadata(dir, "my_key", "my_new_value", "")
		FailError(t, err)
	}

	// search by meta
	entries, err := filesystem.SearchByMeta("my_key", "my_value")
	FailError(t, err)

	numFiles := 0
	numDirs := 0
	for _, entry := range entries {
		if entry.IsDir() {
			assert.Contains(t, dirs1, entry.Path, "meta search result directory should be in first set")
			numDirs++
		} else {
			assert.Contains(t, files1, entry.Path, "meta search result file should be in first set")
			numFiles++
		}
		assert.NotEmpty(t, entry.ID, "search result entry should have iRODS ID")
	}

	assert.Equal(t, len(dirs1), numDirs, "meta search directory count should match first set")
	assert.Equal(t, len(files1), numFiles, "meta search file count should match first set")
}

func testListACLs(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	files, dirs, err := CreateSampleFilesAndDirs(t, server, homeDir, 5, 5)
	FailError(t, err)
	defer func() {
		for _, file := range files {
			err = filesystem.RemoveFile(file, true)
			FailError(t, err)
		}

		for _, dir := range dirs {
			err = filesystem.RemoveDir(dir, true, true)
			FailError(t, err)
		}
	}()

	for _, file := range files {
		acls, err := filesystem.ListACLsWithGroupUsers(file)
		FailError(t, err)

		assert.GreaterOrEqual(t, len(acls), 1, "file should have at least owner's ACL")
		foundOwn := false
		for _, acl := range acls {
			if acl.UserName == account.ClientUser && acl.UserZone == account.ClientZone {
				assert.Equal(t, types.IRODSAccessLevelOwner, acl.AccessLevel, "owner ACL should grant owner access level")
				foundOwn = true
			}

			assert.Equal(t, file, acl.Path, "ACL entry path should match queried file path")
		}

		assert.True(t, foundOwn, "file creator should have owner ACL entry")
	}
}

func testCreateStat(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "testcreate.bin"
	irodsPath := homeDir + "/" + filename

	text := "HELLO WORLD"

	// create
	fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
	FailError(t, err)

	// stat
	stat, err := filesystem.Stat(irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, stat.ID, "stat result should have assigned iRODS ID")
	assert.Equal(t, fs.FileEntry, stat.Type, "stat type should indicate file entry")

	// write
	_, err = fileHandle.Write([]byte(text))
	FailError(t, err)

	// close
	err = fileHandle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(irodsPath), "created file should exist")

	// read
	newFileHandle, err := filesystem.OpenFile(irodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err, "read should reach end of file")

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]), "read bytes should match original written text")

	// stat
	stat, err = filesystem.Stat(irodsPath)
	FailError(t, err)

	assert.Equal(t, int64(len(text)), stat.Size, "stat size should reflect bytes written")

	// delete
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(irodsPath), "file should no longer exist after deletion")
}

func testSpecialCharInFilename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	specialCharacters := []string{
		"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_",
		"=", "+", "{", "}", "[", "]", "|", "\\", ":", ";", "\"", "'",
		"<", ">", ",", "?", "`", "~",
	}

	filenamePattern := "test_special_char_%s.bin"
	text := "HELLO WORLD"

	for _, char := range specialCharacters {
		filename := fmt.Sprintf(filenamePattern, char)
		irodsPath := homeDir + "/" + filename

		// create
		fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
		FailError(t, err)

		// write
		_, err = fileHandle.Write([]byte(text))
		FailError(t, err)

		// close
		err = fileHandle.Close()
		FailError(t, err)

		assert.True(t, filesystem.Exists(irodsPath), "file with special character in name should exist after creation")

		// read
		newFileHandle, err := filesystem.OpenFile(irodsPath, "", "r")
		FailError(t, err)

		buffer := make([]byte, 1024)
		readLen, err := newFileHandle.Read(buffer)
		assert.Equal(t, io.EOF, err, "read should reach end of file")

		err = newFileHandle.Close()
		FailError(t, err)

		assert.Equal(t, text, string(buffer[:readLen]), "read bytes should match written data")

		// stat
		stat, err := filesystem.Stat(irodsPath)
		FailError(t, err)

		assert.Equal(t, filename, stat.Name, "stat name should match created filename")
		assert.Equal(t, int64(len(text)), stat.Size, "stat size should reflect written bytes")

		// delete
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)

		assert.False(t, filesystem.Exists(irodsPath), "special char file should not exist after deletion")
	}
}

func testWriteRename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "testwrite.bin"
	irodsPath := homeDir + "/" + filename

	newFilename := "testrename.bin"
	newIrodsPath := homeDir + "/" + newFilename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
	FailError(t, err)

	// write
	_, err = fileHandle.Write([]byte(text1))
	FailError(t, err)

	// rename
	err = filesystem.RenameFile(irodsPath, newIrodsPath)
	FailError(t, err)

	// write again
	_, err = fileHandle.Write([]byte(text2))
	FailError(t, err)

	// close
	err = fileHandle.Close()
	FailError(t, err)

	assert.False(t, filesystem.Exists(irodsPath), "original file path should not exist after rename")
	assert.True(t, filesystem.Exists(newIrodsPath), "renamed file path should exist after rename")

	// read
	newFileHandle, err := filesystem.OpenFile(newIrodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err, "read should reach end of file")

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]), "read should return all written data")

	// delete
	err = filesystem.RemoveFile(newIrodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newIrodsPath), "renamed file should not exist after deletion")
}

func testWriteRenameDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	dirName := "testdir"
	testDirPath := homeDir + "/" + dirName

	err = filesystem.MakeDir(testDirPath, true)
	FailError(t, err)

	filename := "testwrite.bin"
	irodsPath := testDirPath + "/" + filename

	newDirName := "testdir_rename"
	newTestDirPath := homeDir + "/" + newDirName
	newIrodsPath := newTestDirPath + "/" + filename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
	FailError(t, err)

	// write
	_, err = fileHandle.Write([]byte(text1))
	FailError(t, err)

	// rename
	err = filesystem.RenameDir(testDirPath, newTestDirPath)
	FailError(t, err)

	// write again
	_, err = fileHandle.Write([]byte(text2))
	FailError(t, err)

	// close
	err = fileHandle.Close()
	FailError(t, err)

	assert.False(t, filesystem.Exists(testDirPath), "original directory path should not exist after rename")
	assert.True(t, filesystem.Exists(newTestDirPath), "renamed directory path should exist")

	assert.False(t, filesystem.Exists(irodsPath), "file's original path should not exist after dir rename")
	assert.True(t, filesystem.Exists(newIrodsPath), "file's new path should exist after dir rename")

	// read
	newFileHandle, err := filesystem.OpenFile(newIrodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err, "read should reach end of file")

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]), "read should return all accumulated written data")

	// delete
	err = filesystem.RemoveFile(newIrodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newIrodsPath), "renamed file should not exist after deletion")

	err = filesystem.RemoveDir(newTestDirPath, true, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newTestDirPath), "renamed directory should not exist after deletion")
}

func testRemoveClose(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "testremove.bin"
	irodsPath := homeDir + "/" + filename

	text := "HELLO WORLD!"

	// create
	fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
	FailError(t, err)

	// write
	_, err = fileHandle.Write([]byte(text))
	FailError(t, err)

	// remove
	go func() {
		time.Sleep(3 * time.Second)

		err = fileHandle.Close()
		FailError(t, err)
	}()

	// remove will be blocked until the close is done
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(irodsPath), "file should not exist after concurrent close and remove")
}
