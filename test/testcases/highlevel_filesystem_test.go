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
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
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
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	newDir := fmt.Sprintf("%s/make_dir_recurse", homeDir)

	// get side connection
	conn, err := filesystem.GetMetadataConnection(true)
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
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

func testListDirectory(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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
			assert.Contains(t, dirs, entry.Path)
			numDirs++
		} else {
			assert.Contains(t, files, entry.Path)
			numFiles++
		}
		assert.NotEmpty(t, entry.ID)
	}

	assert.Equal(t, len(dirs), numDirs)
	assert.Equal(t, len(files), numFiles)
}

func testSearchByMeta(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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
			assert.Contains(t, dirs1, entry.Path)
			numDirs++
		} else {
			assert.Contains(t, files1, entry.Path)
			numFiles++
		}
		assert.NotEmpty(t, entry.ID)
	}

	assert.Equal(t, len(dirs1), numDirs)
	assert.Equal(t, len(files1), numFiles)
}

func testListACLs(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	account := server.GetAccountCopy()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

		assert.GreaterOrEqual(t, len(acls), 1)
		foundOwn := false
		for _, acl := range acls {
			if acl.UserName == account.ClientUser && acl.UserZone == account.ClientZone {
				assert.Equal(t, types.IRODSAccessLevelOwner, acl.AccessLevel)
				foundOwn = true
			}

			assert.Equal(t, file, acl.Path)
		}

		assert.True(t, foundOwn)
	}
}

func testCreateStat(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

	filename := "testcreate.bin"
	irodsPath := homeDir + "/" + filename

	text := "HELLO WORLD"

	// create
	fileHandle, err := filesystem.CreateFile(irodsPath, "", "w")
	FailError(t, err)

	// stat
	stat, err := filesystem.Stat(irodsPath)
	FailError(t, err)

	assert.NotEmpty(t, stat.ID)
	assert.Equal(t, fs.FileEntry, stat.Type)

	// write
	_, err = fileHandle.Write([]byte(text))
	FailError(t, err)

	// close
	err = fileHandle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(irodsPath))

	// read
	newFileHandle, err := filesystem.OpenFile(irodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// stat
	stat, err = filesystem.Stat(irodsPath)
	FailError(t, err)

	assert.Equal(t, int64(len(text)), stat.Size)

	// delete
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(irodsPath))
}

func testSpecialCharInFilename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

		assert.True(t, filesystem.Exists(irodsPath))

		// read
		newFileHandle, err := filesystem.OpenFile(irodsPath, "", "r")
		FailError(t, err)

		buffer := make([]byte, 1024)
		readLen, err := newFileHandle.Read(buffer)
		assert.Equal(t, io.EOF, err)

		err = newFileHandle.Close()
		FailError(t, err)

		assert.Equal(t, text, string(buffer[:readLen]))

		// stat
		stat, err := filesystem.Stat(irodsPath)
		FailError(t, err)

		assert.Equal(t, filename, stat.Name)
		assert.Equal(t, int64(len(text)), stat.Size)

		// delete
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)

		assert.False(t, filesystem.Exists(irodsPath))
	}
}

func testWriteRename(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

	assert.False(t, filesystem.Exists(irodsPath))
	assert.True(t, filesystem.Exists(newIrodsPath))

	// read
	newFileHandle, err := filesystem.OpenFile(newIrodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newIrodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newIrodsPath))
}

func testWriteRenameDir(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

	assert.False(t, filesystem.Exists(testDirPath))
	assert.True(t, filesystem.Exists(newTestDirPath))

	assert.False(t, filesystem.Exists(irodsPath))
	assert.True(t, filesystem.Exists(newIrodsPath))

	// read
	newFileHandle, err := filesystem.OpenFile(newIrodsPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newFileHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newFileHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newIrodsPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newIrodsPath))

	err = filesystem.RemoveDir(newTestDirPath, true, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newTestDirPath))
}

func testRemoveClose(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir := test.GetTestHomeDir()

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

	assert.False(t, filesystem.Exists(irodsPath))
}
