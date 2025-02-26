package testcases

/*
var (
	fsIOTestID = xid.New().String()
)

func TestFSIO(t *testing.T) {
	StartIRODSTestServer()
	defer shutdownIRODSTestServer()

	makeHomeDir(t, fsIOTestID)

	t.Run("test UpDownMBFiles", testUpDownMBFiles)
	t.Run("test UpRemoveMBFiles", testUpRemoveMBFiles)
	t.Run("test UpDownMBFilesParallel", testUpDownMBFilesParallel)
	t.Run("test UpDownMBFilesParallelRedirectToResource", testUpDownMBFilesParallelRedirectToResource)
}

func testUpDownMBFiles(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	FailError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	FailError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, false, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFile(iRODSPath, "", localDownloadPath, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		FailError(t, err)

		err = os.Remove(localDownloadPath)
		FailError(t, err)
	}

	err = os.Remove(localPath)
	FailError(t, err)
}

func testUpRemoveMBFiles(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)
	testDirPath := path.Join(homedir, "test_dir")

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	FailError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", testDirPath, path.Base(localPath))

	// core
	for i := 0; i < 100; i++ {
		t.Logf("iteration %d, making dir", i)
		err = filesystem.MakeDir(testDirPath, true)
		FailError(t, err)

		t.Logf("iteration %d, uploading file", i)
		_, err = filesystem.UploadFile(localPath, iRODSPath, "", false, true, true, false, nil)
		FailError(t, err)

		t.Logf("iteration %d, stating file", i)
		fileStat, err := filesystem.Stat(iRODSPath)
		FailError(t, err)

		if fileStat.Size != fileSize {
			FailError(t, fmt.Errorf("wrong size"))
		}

		t.Logf("iteration %d, removing dir", i)
		err = filesystem.RemoveDir(testDirPath, true, true)
		FailError(t, err)

		t.Logf("iteration %d, stating file again", i)
		fileStat2, err := filesystem.Stat(iRODSPath)
		if err != nil {
			if !types.IsFileNotFoundError(err) {
				FailError(t, err)
			}
		} else {
			FailError(t, fmt.Errorf("file not deleted - %q (size %d)", fileStat2.Path, fileStat2.Size))
		}
	}

	err = os.Remove(localPath)
	FailError(t, err)
}

func testUpDownMBFilesParallel(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	FailError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	FailError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFileParallel(localPath, iRODSPath, "", 0, false, true, true, false, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFileParallel(iRODSPath, "", localDownloadPath, 0, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		FailError(t, err)

		err = os.Remove(localDownloadPath)
		FailError(t, err)
	}

	err = os.Remove(localPath)
	FailError(t, err)
}

func testUpDownMBFilesParallelRedirectToResource(t *testing.T) {
	account := GetIRODSTestServerAccount()

	account.ClientServerNegotiation = false

	fsConfig := GetTestFileSystemConfig()

	filesystem, err := fs.NewFileSystem(account, fsConfig)
	FailError(t, err)
	defer filesystem.Release()

	homedir := getHomeDir(fsIOTestID)

	fileSize := int64(100 * 1024 * 1024) // 100MB
	localPath, err := createLocalTestFile("test_file_", fileSize)
	FailError(t, err)

	iRODSPath := fmt.Sprintf("%s/%s", homedir, path.Base(localPath))
	localDownloadPath, err := filepath.Abs(fmt.Sprintf("./%s", filepath.Base(localPath)))
	FailError(t, err)

	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = filesystem.UploadFileParallelRedirectToResource(localPath, iRODSPath, "", 0, false, true, true, false, nil)
		duration := time.Since(start)

		t.Logf("upload a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		start = time.Now()
		_, err = filesystem.DownloadFileParallelResumable(iRODSPath, "", localDownloadPath, 0, true, nil)
		duration = time.Since(start)

		t.Logf("download a file in size %d took time - %v", fileSize, duration)
		FailError(t, err)

		// remove
		err = filesystem.RemoveFile(iRODSPath, true)
		FailError(t, err)

		err = os.Remove(localDownloadPath)
		FailError(t, err)
	}

	err = os.Remove(localPath)
	FailError(t, err)
}
*/
