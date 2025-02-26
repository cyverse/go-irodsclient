package testcases

/*
var (
	redirectionToResourceAPITestID = xid.New().String()
)

func TestRedirectionToResourceAPI(t *testing.T) {
	StartIRODSTestServer()
	defer shutdownIRODSTestServer()

	log.SetLevel(log.DebugLevel)

	makeHomeDir(t, redirectionToResourceAPITestID)

	t.Run("test DownloadDataObjectFromResourceServer", testDownloadDataObjectFromResourceServer)
	t.Run("test UploadDataObjectToResourceServer", testUploadDataObjectToResourceServer)
}

func testDownloadDataObjectFromResourceServer(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	FailError(t, err)

	homedir := getHomeDir(redirectionToResourceAPITestID)

	// gen very large file
	filename := "test_large_file.bin"
	fileSize := 100 * 1024 * 1024 // 100MB

	filepath, err := createLocalTestFile(filename, int64(fileSize))
	FailError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectParallel(sess, filepath, irodsPath, "", 4, false, nil, callBack)
	FailError(t, err)
	assert.Greater(t, callbackCalled, 3) // at least called 3 times

	checksumOriginal, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	err = os.Remove(filepath)
	FailError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	FailError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	keywords := map[common.KeyWord]string{
		common.VERIFY_CHKSUM_KW: "",
	}

	checksum, err := fs.DownloadDataObjectFromResourceServer(sess, irodsPath, "", filename, int64(fileSize), 0, keywords, callBack)
	FailError(t, err)

	assert.NotEmpty(t, checksum)

	checksumNew, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	err = os.Remove(filename)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)

	assert.Equal(t, checksumOriginal, checksumNew)

	checksumAlg, checksumStr, err := types.ParseIRODSChecksumString(checksum)
	FailError(t, err)
	assert.Equal(t, checksumAlg, types.ChecksumAlgorithmSHA256)
	assert.Equal(t, checksumNew, checksumStr)

	sess.ReturnConnection(conn)
}

func testUploadDataObjectToResourceServer(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection()
	FailError(t, err)

	homedir := getHomeDir(redirectionToResourceAPITestID)

	// gen very large file
	filename := "test_large_file.bin"
	//fileSize := 100 * 1024 * 1024 // 100MB
	fileSize := 5 * 1024 * 1024 * 1024 // 5GB

	filepath, err := createLocalTestFile(filename, int64(fileSize))
	FailError(t, err)

	// upload
	irodsPath := homedir + "/" + filename

	callbackCalled := 0
	callBack := func(current int64, total int64) {
		callbackCalled++
	}

	err = fs.UploadDataObjectToResourceServer(sess, filepath, irodsPath, "", 0, false, nil, callBack)
	FailError(t, err)
	assert.Greater(t, callbackCalled, 3) // at least called 3 times

	checksumOriginal, err := util.HashLocalFile(filepath, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	err = os.Remove(filepath)
	FailError(t, err)

	coll, err := fs.GetCollection(conn, homedir)
	FailError(t, err)

	obj, err := fs.GetDataObject(conn, coll, filename)
	FailError(t, err)

	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, int64(fileSize), obj.Size)

	// get
	keywords := map[common.KeyWord]string{
		common.VERIFY_CHKSUM_KW: "",
	}

	checksum, err := fs.DownloadDataObjectFromResourceServer(sess, irodsPath, "", filename, int64(fileSize), 0, keywords, callBack)
	FailError(t, err)

	assert.NotEmpty(t, checksum)

	checksumNew, err := util.HashLocalFile(filename, string(types.ChecksumAlgorithmSHA256))
	FailError(t, err)

	err = os.Remove(filename)
	FailError(t, err)

	// delete
	err = fs.DeleteDataObject(conn, irodsPath, true)
	FailError(t, err)

	assert.Equal(t, checksumOriginal, checksumNew)

	checksumAlg, checksumStr, err := types.ParseIRODSChecksumString(checksum)
	FailError(t, err)
	assert.Equal(t, checksumAlg, types.ChecksumAlgorithmSHA256)
	assert.Equal(t, checksumNew, checksumStr)

	sess.ReturnConnection(conn)
}
*/
