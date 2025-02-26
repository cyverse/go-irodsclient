package testcases

/*
var (
	fsTestID = xid.New().String()
)

func TestFS(t *testing.T) {
	StartIRODSTestServer(t)
	defer shutdownIRODSTestServer()

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
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	entries, err := filesystem.List(homedir)
	FailError(t, err)

	entryPaths := []string{}

	for _, entry := range entries {
		entryPaths = append(entryPaths, entry.Path)
		assert.NotEmpty(t, entry.ID)
	}

	expected := []string{}
	expected = append(expected, GetSampleDirs()...)
	expected = append(expected, GetSampleFiles()...)

	assert.Equal(t, len(entries), len(expected))
	assert.ElementsMatch(t, entryPaths, expected)
}

func testListEntriesByMeta(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	for _, testFilePath := range GetSampleFiles() {
		sha1sum := sha1.New()
		_, err = sha1sum.Write([]byte(testFilePath))
		FailError(t, err)

		hashBytes := sha1sum.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)

		entries, err := filesystem.SearchByMeta("hash", hashString)
		FailError(t, err)

		assert.Equal(t, 1, len(entries))
		assert.Equal(t, testFilePath, entries[0].Path)
	}
}

func testListACLs(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	objectPath := GetSampleFiles()[0]

	acls, err := filesystem.ListACLsWithGroupUsers(objectPath)
	FailError(t, err)

	assert.GreaterOrEqual(t, len(acls), 1)
	for _, acl := range acls {
		assert.Equal(t, objectPath, acl.Path)
	}
}

func testReadWrite(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!<?!'\">"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	_, err = handle.Write([]byte(text))
	FailError(t, err)

	err = handle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	FailError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testCreateStat(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	// stat
	stat, err := filesystem.Stat(newDataObjectPath)
	FailError(t, err)
	assert.NotEmpty(t, stat.ID)
	assert.Equal(t, fs.FileEntry, stat.Type)

	// write
	_, err = handle.Write([]byte(text))
	FailError(t, err)

	// close
	err = handle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	FailError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testSpecialCharInName(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_special_char_&@#^%\\_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	FailError(t, err)

	// close
	err = handle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPath))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPath, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	FailError(t, err)

	assert.Equal(t, text, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPath, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPath))
}

func testWriteRename(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
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
	FailError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	FailError(t, err)

	// rename
	err = filesystem.RenameFile(newDataObjectPath, newDataObjectPathRenameTarget)
	FailError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	FailError(t, err)

	// close
	err = handle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))
}

func testWriteRenameDir(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)
	newdir := fmt.Sprintf("%s/testdir_%s", homedir, xid.New().String())

	err = filesystem.MakeDir(newdir, true)
	FailError(t, err)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := newdir + "/" + newDataObjectFilename

	newdirRenameTarget := fmt.Sprintf("%s/testdir_%s", homedir, xid.New().String())
	newDataObjectPathRenameTarget := newdirRenameTarget + "/" + newDataObjectFilename

	text1 := "HELLO"
	text2 := " WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	// write
	_, err = handle.Write([]byte(text1))
	FailError(t, err)

	// rename
	err = filesystem.RenameDir(newdir, newdirRenameTarget)
	FailError(t, err)

	// write again
	_, err = handle.Write([]byte(text2))
	FailError(t, err)

	// close
	err = handle.Close()
	FailError(t, err)

	assert.True(t, filesystem.Exists(newDataObjectPathRenameTarget))

	// read
	newHandle, err := filesystem.OpenFile(newDataObjectPathRenameTarget, "", "r")
	FailError(t, err)

	buffer := make([]byte, 1024)
	readLen, err := newHandle.Read(buffer)
	assert.Equal(t, io.EOF, err)

	err = newHandle.Close()
	FailError(t, err)

	assert.Equal(t, text1+text2, string(buffer[:readLen]))

	// delete
	err = filesystem.RemoveFile(newDataObjectPathRenameTarget, true)
	FailError(t, err)

	assert.False(t, filesystem.Exists(newDataObjectPathRenameTarget))

	err = filesystem.RemoveDir(newdirRenameTarget, true, true)
	FailError(t, err)
}

func testRemoveClose(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsTestID)

	newDataObjectFilename := "testobj_" + xid.New().String()
	newDataObjectPath := homedir + "/" + newDataObjectFilename

	text := "HELLO WORLD!"

	// create
	handle, err := filesystem.CreateFile(newDataObjectPath, "", "w")
	FailError(t, err)

	// write
	_, err = handle.Write([]byte(text))
	FailError(t, err)

	// remove
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		//t.Logf("calling remove %q\n", newDataObjectPath)
		err = filesystem.RemoveFile(newDataObjectPath, true)
		FailError(t, err)
		wg.Done()
	}()

	go func() {
		time.Sleep(3 * time.Second)
		// close
		//t.Logf("calling close %q\n", newDataObjectPath)
		err = handle.Close()
		FailError(t, err)
		wg.Done()
	}()

	wg.Wait()
	assert.False(t, filesystem.Exists(newDataObjectPath))
}
*/
