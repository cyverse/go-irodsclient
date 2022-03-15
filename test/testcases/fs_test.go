package testcases

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/stretchr/testify/assert"
)

func TestFS(t *testing.T) {
	setup()
	defer shutdown()

	t.Run("test PrepareSamples", testPrepareSamples)

	t.Run("test ListEntries", testListEntries)
	t.Run("test ListEntriesByMeta", testListEntriesByMeta)
	t.Run("test ListACLs", testListACLs)
	t.Run("test ReadWrite", testReadWrite)
	t.Run("test WriteRename", testWriteRename)
	t.Run("test WriteRenameDir", testWriteRenameDir)
}

func testListEntries(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	collections, err := fs.List(homedir)
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

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	for _, testFilePath := range GetTestFiles() {
		sha1sum := sha1.New()
		_, err = sha1sum.Write([]byte(testFilePath))
		assert.NoError(t, err)

		hashBytes := sha1sum.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)

		entries, err := fs.SearchByMeta("hash", hashString)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(entries))
		assert.Equal(t, testFilePath, entries[0].Path)
	}
}

func testListACLs(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	objectPath := GetTestFiles()[0]

	acls, err := fs.ListACLsWithGroupUsers(objectPath)
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

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	newDataObjectFilename := "testobj123"
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!<?!'\">"

	// create
	handle, err := fs.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	err = handle.Write([]byte(text))
	assert.NoError(t, err)

	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, fs.Exists(newDataObjectPath))

	// read
	newHandle, err := fs.OpenFile(newDataObjectPath, "", "r")
	assert.NoError(t, err)

	readData, err := newHandle.Read(1024)
	assert.NoError(t, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text, string(readData))

	// delete
	err = fs.RemoveFile(newDataObjectPath, true)
	assert.NoError(t, err)

	assert.False(t, fs.Exists(newDataObjectPath))
}

func testWriteRename(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	newDataObjectFilename := "testobj1234"
	newDataObjectPath := homedir + "/" + newDataObjectFilename
	newDataObjectPathRenameTarget := homedir + "/" + newDataObjectFilename + "_new"

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := fs.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	err = handle.Write([]byte(text1))
	assert.NoError(t, err)

	// rename
	err = fs.RenameFile(newDataObjectPath, newDataObjectPathRenameTarget)
	assert.NoError(t, err)

	fmt.Printf("rename - %s done\n", newDataObjectPathRenameTarget)

	// write again
	err = handle.Write([]byte(text2))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, fs.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := fs.OpenFile(newDataObjectPathRenameTarget, "", "r")
	assert.NoError(t, err)

	readData, err := newHandle.Read(1024)
	assert.NoError(t, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text1+text2, string(readData))

	// delete
	err = fs.RemoveFile(newDataObjectPathRenameTarget, true)
	assert.NoError(t, err)

	assert.False(t, fs.Exists(newDataObjectPathRenameTarget))
}

func testWriteRenameDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)
	newdir := fmt.Sprintf("%s/testdir", homedir)

	err = fs.MakeDir(newdir, true)
	assert.NoError(t, err)

	newDataObjectFilename := "testobj1234"
	newDataObjectPath := newdir + "/" + newDataObjectFilename

	newdirRenameTarget := newdir + "_renamed"
	newDataObjectPathRenameTarget := newdirRenameTarget + "/" + newDataObjectFilename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := fs.CreateFile(newDataObjectPath, "", "w")
	assert.NoError(t, err)

	// write
	err = handle.Write([]byte(text1))
	assert.NoError(t, err)

	// rename
	err = fs.RenameDir(newdir, newdirRenameTarget)
	assert.NoError(t, err)

	fmt.Printf("rename dir - %s done\n", newdirRenameTarget)

	// write again
	err = handle.Write([]byte(text2))
	assert.NoError(t, err)

	// close
	err = handle.Close()
	assert.NoError(t, err)

	assert.True(t, fs.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := fs.OpenFile(newDataObjectPathRenameTarget, "", "r")
	assert.NoError(t, err)

	readData, err := newHandle.Read(1024)
	assert.NoError(t, err)

	err = newHandle.Close()
	assert.NoError(t, err)

	assert.Equal(t, text1+text2, string(readData))

	// delete
	err = fs.RemoveFile(newDataObjectPathRenameTarget, true)
	assert.NoError(t, err)

	assert.False(t, fs.Exists(newDataObjectPathRenameTarget))

	err = fs.RemoveDir(newdirRenameTarget, true, true)
	assert.NoError(t, err)
}
