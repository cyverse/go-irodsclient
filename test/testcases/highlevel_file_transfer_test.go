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

// file sizes up to common.MaxSizeForSingleBufferTransfer (32MB) are uploaded in a single buffer,
// larger ones are uploaded through the regular transfer
var highlevelFileTransferTestFileSizes = []int64{
	0,
	16 * 1024 * 1024,  // 16MB
	32 * 1024 * 1024,  // 32MB
	100 * 1024 * 1024, // 100MB
	200 * 1024 * 1024, // 200MB
	300 * 1024 * 1024, // 300MB
}

// same as highlevelFileTransferTestFileSizes, but in multiples of 1000
var highlevelFileTransferTest1000sFileSizes = []int64{
	0,
	16 * 1000 * 1000,  // 16MB
	32 * 1000 * 1000,  // 32MB
	100 * 1000 * 1000, // 100MB
	200 * 1000 * 1000, // 200MB
	300 * 1000 * 1000, // 300MB
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

	for i, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size(), "downloaded file size should match original file size")

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

	for _, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
	}

	for i := len(highlevelFileTransferTestFileSizes) - 2; i >= 0; i-- {
		// gen large file
		fileSize := highlevelFileTransferTestFileSizes[i] // 200, 100, 32, 16, 0 MB (300MB was uploaded last above)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
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

	for i, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())

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

	for _, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
	}

	for i := len(highlevelFileTransferTestFileSizes) - 2; i >= 0; i-- {
		// gen large file
		fileSize := highlevelFileTransferTestFileSizes[i] // 200, 100, 32, 16, 0 MB (300MB was uploaded last above)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileParallel(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
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

	for i, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size(), "downloaded file size should match original file size")

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

	for _, fileSize := range highlevelFileTransferTestFileSizes {
		// gen large file
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
	}

	for i := len(highlevelFileTransferTestFileSizes) - 2; i >= 0; i-- {
		// gen large file
		fileSize := highlevelFileTransferTestFileSizes[i] // 200, 100, 32, 16, 0 MB (300MB was uploaded last above)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		st, err := os.Stat(localPath)
		FailError(t, err)
		assert.Equal(t, fileSize, st.Size(), "created local test file size should match the expected file size")

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size())
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

	for i, fileSize := range highlevelFileTransferTest1000sFileSizes {
		// gen large file
		filename := fmt.Sprintf("test_large_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFileRedirectToResource(localPath, irodsPath, "", 0, false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size(), "downloaded file size should match original file size")

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

	for i, fileSize := range highlevelFileTransferTestFileSizes {
		// gen file
		filename := fmt.Sprintf("test_callback_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size(), "downloaded file size should match original file size")

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

	for i, fileSize := range highlevelFileTransferTestFileSizes {
		// gen file
		filename := fmt.Sprintf("test_callback_parallel_file_%d.bin", i)
		localPath, err := CreateLocalTestFile(t, filename, fileSize)
		FailError(t, err)

		irodsPath := homeDir + "/" + filename

		_, err = filesystem.UploadFile(localPath, irodsPath, "", false, true, nil)
		FailError(t, err)

		entry, err := filesystem.Stat(irodsPath)
		FailError(t, err)
		assert.Equal(t, filename, entry.Name, "stat name should match uploaded filename")
		assert.Equal(t, fileSize, entry.Size, "stat size should match source file size")
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
		assert.Equal(t, fileSize, st.Size(), "downloaded file size should match original file size")

		// remove new local file
		err = os.Remove(newLocalPath)
		FailError(t, err)

		// remove irods file
		err = filesystem.RemoveFile(irodsPath, true)
		FailError(t, err)
	}
}
