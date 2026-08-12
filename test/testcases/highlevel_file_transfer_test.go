package testcases

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/stretchr/testify/assert"
)

func getHighlevelFileTransferTest() Test {
	return Test{
		Name: "Highlevel_FileTransfer",
		Func: highlevelFileTransferTest,
	}
}

func highlevelFileTransferTest(t *testing.T, test *Test) {
	t.Run("UploadAndDownload", testUploadAndDownload)
	t.Run("UploadAndDownloadOverwrite", testUploadAndDownloadOverwrite)
	t.Run("UploadAndDownloadParallel", testUploadAndDownloadParallel)
	t.Run("UploadAndDownloadParallelOverwrite", testUploadAndDownloadParallelOverwrite)
	t.Run("UploadAndDownloadRedirectToResource", testUploadAndDownloadRedirectToResource)
	t.Run("UploadAndDownloadRedirectToResourceOverwrite", testUploadAndDownloadRedirectToResourceOverwrite)
	t.Run("UploadAndDownload1000sRedirectToResource", testUploadAndDownload1000sRedirectToResource)
	t.Run("DownloadWithCallback", testDownloadWithCallback)
	t.Run("DownloadWithCallbackParallel", testDownloadWithCallbackParallel)
}

func testUploadAndDownload(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFile(irodsPath, "", newLocalPath, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i <= 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFile(irodsPath, "", newLocalPath, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	for i := 2; i >= 0; i-- {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 200, 100, 0 MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFile(irodsPath, "", newLocalPath, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownloadParallel(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileParallel(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadParallelOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i < 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileParallel(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	for i := 2; i >= 0; i-- {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 200, 100, 0 MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileParallel(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownloadRedirectToResource(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testUploadAndDownloadRedirectToResourceOverwrite(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	filename := "test_large_file.bin"
	newLocalPath := t.TempDir() + "/new_test_large_file.bin"
	irodsPath := homeDir + "/" + filename

	for i := 0; i < 3; i++ {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	for i := 2; i >= 0; i-- {
		// gen large file
		fileSize := i * 100 * 1024 * 1024 // 200, 100, 0 MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err = os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size())
	}

	// remove new local file
	err = os.Remove(newLocalPath)
	FailError(t, err)

	// remove irods file
	err = filesystem.RemoveFile(irodsPath, true)
	FailError(t, err)
}

func testUploadAndDownload1000sRedirectToResource(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()
	serverInfo := server.GetInfo()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 0; i < 3; i++ {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		fileSize := i * 100 * 1000 * 1000 // 0, 100, 200, 300... MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_large_file_%d.bin", i)
		// turn compareChecksum off, not generated synchronously in v4.2.8
		compareChecksum := true
		if serverInfo.Version == "4.2.8" {
			compareChecksum = false
		}
		_, err = filesystem.DownloadFileRedirectToResource(irodsPath, "", newLocalPath, 0, compareChecksum, nil)
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testDownloadWithCallback(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 1; i <= 3; i++ {
		// gen file
		filename := fmt.Sprintf("test_callback_file_%d.bin", i)
		fileSize := i * 10 * 1024 * 1024 // 10, 20, 30 MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// download with callback
		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_callback_file_%d.bin", i)
		f, err := os.Create(newLocalPath)
		FailError(t, err)

		var mu sync.Mutex

		blockSize := 1 * 1024 * 1024 // 1MB
		numBlocks := 4

		blockReadyCallback := func(data []byte, offset int64) error {
			mu.Lock()
			defer mu.Unlock()

			_, writeErr := f.WriteAt(data, offset)
			return writeErr
		}

		_, err = filesystem.DownloadFileWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, nil)
		FailError(t, err)

		err = f.Close()
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}

func testDownloadWithCallbackParallel(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	filesystem, err := server.GetFileSystem()
	FailError(t, err)
	defer filesystem.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	for i := 1; i <= 3; i++ {
		// gen file
		filename := fmt.Sprintf("test_callback_parallel_file_%d.bin", i)
		fileSize := i * 10 * 1024 * 1024 // 10, 20, 30 MB
		localPath, err := CreateLocalTestFile(t, filename, int64(fileSize))
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, int64(fileSize), entry.Size, "stat size should match source file size")
		assert.Equal(t, fs.FileEntry, entry.Type, "stat type should indicate file entry")

		// remove local file
		err = os.Remove(localPath)
		FailError(t, err)

		// download with callback parallel
		newLocalPath := t.TempDir() + fmt.Sprintf("/new_test_callback_parallel_file_%d.bin", i)
		f, err := os.Create(newLocalPath)
		FailError(t, err)

		var mu sync.Mutex

		blockSize := 1 * 1024 * 1024 // 1MB
		numBlocks := 8

		blockReadyCallback := func(data []byte, offset int64) error {
			mu.Lock()
			defer mu.Unlock()

			_, writeErr := f.WriteAt(data, offset)
			return writeErr
		}

		_, err = filesystem.DownloadFileParallelWithCallback(irodsPath, "", blockSize, numBlocks, blockReadyCallback, 0, nil)
		FailError(t, err)

		err = f.Close()
		FailError(t, err)

		st, err := os.Stat(newLocalPath)
		FailError(t, err)
		assert.Equal(t, int64(fileSize), st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}
