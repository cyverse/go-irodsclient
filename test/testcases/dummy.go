package testcases

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/rs/xid"
	"golang.org/x/xerrors"
)

func MakeFixedContentDataBuf(size int64) []byte {
	testval := "abcdefghijklmnopqrstuvwxyz"

	// fill
	dataBuf := make([]byte, size)
	writeLen := 0
	for writeLen < len(dataBuf) {
		copy(dataBuf[writeLen:], testval)
		writeLen += len(testval)
	}
	return dataBuf
}

func MakeRandomContentDataBuf(size int64) []byte {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") //62

	// fill
	dataBuf := make([]byte, size)
	for i := range dataBuf {
		dataBuf[i] = letters[rand.Intn(len(letters))]
	}
	return dataBuf
}

func CreateLocalTestFile(t *testing.T, name string, size int64) (string, error) {
	// fill
	dataBuf := MakeFixedContentDataBuf(1024)

	tempdir := t.TempDir()

	f, err := os.CreateTemp(tempdir, name)
	if err != nil {
		return "", err
	}

	tempPath := f.Name()

	defer f.Close()

	totalWriteLen := int64(0)
	for totalWriteLen < size {
		writeLen, err := f.Write(dataBuf)
		if err != nil {
			os.Remove(tempPath)
			return "", err
		}

		totalWriteLen += int64(writeLen)
	}

	return tempPath, nil
}

func CreateSampleFilesAndDirs(t *testing.T, server *server.IRODSTestServer, dest string, numFiles int, numDirs int) ([]string, []string, error) {
	fs, err := server.GetFileSystem()
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create a new filesystem: %w", err)
	}
	defer fs.Release()

	if !fs.ExistsDir(dest) {
		return nil, nil, xerrors.Errorf("dest directory %q does not exist", dest)
	}

	sampleFiles := []string{}
	sampleDirs := []string{}

	// create random files
	baseRecord := 100
	id := xid.New().String()

	for i := 0; i < numFiles; i++ {
		filesize := int64((i + 1) * 62 * baseRecord)
		filename := fmt.Sprintf("test_file_%s_%d.bin", id, filesize)

		tempPath, err := CreateLocalTestFile(t, filename, filesize)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to create a local test file: %w", err)
		}

		irodsPath := dest + "/" + filename
		_, err = fs.UploadFile(tempPath, irodsPath, "", false, false, false, false, nil)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to upload a data object: %w", err)
		}

		err = os.Remove(tempPath)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to remove a local test file %q: %w", tempPath, err)
		}

		sampleFiles = append(sampleFiles, irodsPath)
	}

	// create random directories
	for i := 0; i < numDirs; i++ {
		dirname := fmt.Sprintf("test_dir_%s_%d", id, i)

		irodsPath := dest + "/" + dirname
		err = fs.MakeDir(irodsPath, true)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to make a directory: %w", err)
		}

		sampleDirs = append(sampleDirs, irodsPath)
	}

	return sampleFiles, sampleDirs, nil
}
