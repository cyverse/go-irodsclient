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

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	collections, err := filesystem.List(homedir)
	assert.NoError(t, err)

	collectionPaths := []string{}

	for _, collection := range collections {
		collectionPaths = append(collectionPaths, collection.Path)
		assert.NotEmpty(t, collection.ID)
	}

	expected := []string{}
	expected = append(expected, GetTestDirs()...)
	expected = append(expected, GetTestFiles()...)

	assert.Equal(t, len(collections), len(expected))
	assert.ElementsMatch(t, collectionPaths, expected)
}

func testListEntriesByMeta(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	for _, testFilePath := range GetTestFiles() {
		sha1sum := sha1.New()
		_, err = sha1sum.Write([]byte(testFilePath))
		assert.NoError(t, err)

		hashBytes := sha1sum.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)

		entries, err := filesystem.SearchByMeta("hash", hashString)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(entries))
		assert.Equal(t, testFilePath, entries[0].Path)
	}
}

func testListACLs(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	objectPath := GetTestFiles()[0]

	acls, err := filesystem.ListACLsWithGroupUsers(objectPath)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, len(acls), 1)
	for _, acl := range acls {
		assert.Equal(t, objectPath, acl.Path)
	}
}

func testReadWrite(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!<?!'\">"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	_, err = handle.Write([]byte(text))
	assert.NoError(t, err)

	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testCreateStat(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_create1234"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// stat
	stat, err := filesystem.Stat(newDataObjectPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, stat.ID)
	assert.Equal(t, fs.FileEntry, stat.Type)

	// write
	_, err = handle.Write([]byte(text))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testSpecialCharInName(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_special_char_&@#^%_1234"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testWriteRename(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj1234"
	newDataObjectPath := homedir + "/" + newDataObjectFilename
	newDataObjectPathRenameTarget := homedir + "/" + newDataObjectFilename + "_new"

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	assert.NoError(t, err)

	// rename
	err = filesystem.RenameFile(newDataObjectPath, newDataObjectPathRenameTarget)
	assert.NoError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))
}

func testWriteRenameDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)
	newdir := fmt.Sprintf("%s/testdir", homedir)

	err = filesystem.MakeDir(newdir, true)
	assert.NoError(t, err)

	newDataObjectFilename := "testobj1234"
	newDataObjectPath := newdir + "/" + newDataObjectFilename

	newdirRenameTarget := newdir + "_renamed"
	newDataObjectPathRenameTarget := newdirRenameTarget + "/" + newDataObjectFilename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	assert.NoError(t, err)

	// rename
	err = filesystem.RenameDir(newdir, newdirRenameTarget)
	assert.NoError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	assert.NoError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	assert.NoError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))

	err = filesystem.RemoveDir(newdirRenameTarget, true, true)
	assert.NoError(t, err)
}

func testRemoveClose(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj1234"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	assert.NoError(t, err)

	// remove
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		//t.Logf("calling remove - %s\n", newDataObjectPath)
		err = filesystem.RemoveFile(newDataObjectPath, true)
		assert.NoError(t, err)
		wg.Done()
	}()

	go func() {
		time.Sleep(3 * time.Second)
		// close
		//t.Logf("calling close - %s\n", newDataObjectPath)
		err = handle.Close()
		assert.NoError(t, err)
		wg.Done()
	}()

	wg.Wait()
	assert.False(t, filesystem.Exists(newDataObjectPath))
}
