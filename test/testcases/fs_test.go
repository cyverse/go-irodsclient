package testcases

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	fsTestID = xid.New().String()
)

func TestFS(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, fsTestID)

	t.Run("test PrepareSamples", testPrepareSamplesForFS)
	t.Run("test ListEntries", testListEntries)
	t.Run("test ListEntriesByMeta", testListEntriesByMeta)
	t.Run("test ListACLs", testListACLs)
	t.Run("test ReadWrite", testReadWrite)
	t.Run("test CreateStat", testCreateStat)
	t.Run("test SpecialCharInName", testSpecialCharInName)
	t.Run("test WriteRename", testWriteRename)
	t.Run("test WriteRenameDir", testWriteRenameDir)
	t.Run("test RemoveClose", testRemoveClose)
}

func testPrepareSamplesForFS(t *testing.T) {
	prepareSamples(t, fsTestID)
}

func testListEntries(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	entries, err := filesystem.List(homedir)
	failError(t, err)

	entryPaths := []string{}

	for _, entry := range entries {
		entryPaths = append(entryPaths, entry.Path)
		assert.NotEmpty(t, entry.ID)
	}

	expected := []string{}
	expected = append(expected, GetTestDirs()...)
	expected = append(expected, GetTestFiles()...)

	assert.Equal(t, len(entries), len(expected))
	assert.ElementsMatch(t, entryPaths, expected)
}

func testListEntriesByMeta(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	for _, testFilePath := range GetTestFiles() {
		sha1sum := sha1.New()
		_, err = sha1sum.Write([]byte(testFilePath))
		failError(t, err)

		hashBytes := sha1sum.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)

		entries, err := filesystem.SearchByMeta("hash", hashString)
		failError(t, err)

		assert.Equal(t, 1, len(entries))
		assert.Equal(t, testFilePath, entries[0].Path)
	}
}

func testListACLs(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	objectPath := GetTestFiles()[0]

	acls, err := filesystem.ListACLsWithGroupUsers(objectPath)
	failError(t, err)

	assert.GreaterOrEqual(t, len(acls), 1)
	for _, acl := range acls {
		assert.Equal(t, objectPath, acl.Path)
	}
}

func testReadWrite(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!<?!'\">"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	_, err = handle.Write([]byte(text))
	failError(t, err)

	err = handle.Close()
	failError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	failError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	failError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	failError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testCreateStat(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	// stat
	stat, err := filesystem.Stat(newDataObjectPath)
	failError(t, err)
	assert.NotEmpty(t, stat.ID)
	assert.Equal(t, fs.FileEntry, stat.Type)

	// write
	_, err = handle.Write([]byte(text))
	failError(t, err)

	// close
	err = handle.Close()
	failError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	failError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	failError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	failError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testSpecialCharInName(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_special_char_&@#^%\\_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	failError(t, err)

	// close
	err = handle.Close()
	failError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	failError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	failError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	failError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testWriteRename(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	newDataObjectPathRenamed := "testobj_" + xid.New().String()
	newDataObjectPathRenameTarget := homedir + "/" + newDataObjectPathRenamed

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	failError(t, err)

	// rename
	err = filesystem.RenameFile(newDataObjectPath, newDataObjectPathRenameTarget)
	failError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	failError(t, err)

	// close
	err = handle.Close()
	failError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	failError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	failError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	failError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))
}

func testWriteRenameDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)
	newdir := fmt.Sprintf("%s/testdir_%s", homedir, xid.New().String())

	err = filesystem.MakeDir(newdir, true)
	failError(t, err)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := newdir + "/" + newDataObjectFilename

	newdirRenameTarget := fmt.Sprintf("%s/testdir_%s", homedir, xid.New().String())
	newDataObjectPathRenameTarget := newdirRenameTarget + "/" + newDataObjectFilename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	failError(t, err)

	// rename
	err = filesystem.RenameDir(newdir, newdirRenameTarget)
	failError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	failError(t, err)

	// close
	err = handle.Close()
	failError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	failError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	failError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	failError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))

	err = filesystem.RemoveDir(newdirRenameTarget, true, true)
	failError(t, err)
}

func testRemoveClose(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	failError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	failError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	failError(t, err)

	// remove
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		//t.Logf("calling remove %q\n", newDataObjectPath)
		err = filesystem.RemoveFile(newDataObjectPath, true)
		failError(t, err)
		wg.Done()
	}()

	go func() {
		time.Sleep(3 * time.Second)
		// close
		//t.Logf("calling close %q\n", newDataObjectPath)
		err = handle.Close()
		failError(t, err)
		wg.Done()
	}()

	wg.Wait()
	assert.False(t, filesystem.Exists(newDataObjectPath))
}
