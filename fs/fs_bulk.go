package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

const (
	defaultChecksumAlgorithm types.ChecksumAlgorithm = types.ChecksumAlgorithmMD5
)

// FileTransferResult a file transfer result
type FileTransferResult struct {
	IRODSCheckSumAlgorithm types.ChecksumAlgorithm `json:"irods_checksum_algorithm"`
	IRODSPath              string                  `json:"irods_path"`
	IRODSCheckSum          []byte                  `json:"irods_checksum"`
	IRODSSize              int64                   `json:"irods_size"`
	LocalCheckSumAlgorithm types.ChecksumAlgorithm `json:"local_checksum_algorithm"`
	LocalPath              string                  `json:"local_path"`
	LocalCheckSum          []byte                  `json:"local_checksum"`
	LocalSize              int64                   `json:"local_size"`
	StartTime              time.Time               `json:"start_time"`
	EndTime                time.Time               `json:"end_time"`
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObject(fs.ioSession, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileWithConnection downloads a file to local
func (fs *FileSystem) DownloadFileWithConnection(conn *connection.IRODSConnection, irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectWithConnection(conn, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileResumable downloads a file to local with support of transfer resume
func (fs *FileSystem) DownloadFileResumable(irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectResumable(fs.ioSession, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileResumableWithConnection downloads a file to local with support of transfer resume
func (fs *FileSystem) DownloadFileResumableWithConnection(conn *connection.IRODSConnection, irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectResumableWithConnection(conn, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileToBuffer downloads a file to buffer
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer *bytes.Buffer, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectToBuffer(fs.ioSession, entry.ToDataObject(), resource, buffer, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	fileTransferResult.LocalSize = int64(buffer.Len())

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileToBufferWithConnection downloads a file to buffer
func (fs *FileSystem) DownloadFileToBufferWithConnection(conn *connection.IRODSConnection, irodsPath string, resource string, buffer *bytes.Buffer, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectToBufferWithConnection(conn, entry.ToDataObject(), resource, buffer, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	fileTransferResult.LocalSize = int64(buffer.Len())

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileParallel downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallel(fs.ioSession, entry.ToDataObject(), resource, localFilePath, taskNum, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileParallelWithConnections downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallelWithConnections(conns []*connection.IRODSConnection, irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallelWithConnections(conns, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileParallelResumable downloads a file to local in parallel with support of transfer resume
func (fs *FileSystem) DownloadFileParallelResumable(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallelResumable(fs.ioSession, entry.ToDataObject(), resource, localFilePath, taskNum, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileParallelResumableWithConnections downloads a file to local in parallel with support of transfer resume
func (fs *FileSystem) DownloadFileParallelResumableWithConnections(conns []*connection.IRODSConnection, irodsPath string, resource string, localPath string, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		// verify checksum
		if len(entry.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %q", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallelResumableWithConnections(conns, entry.ToDataObject(), resource, localFilePath, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileRedirectToResource downloads a file from resource to local in parallel
func (fs *FileSystem) DownloadFileRedirectToResource(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectFromResourceServer(fs.ioSession, entry.ToDataObject(), resource, localFilePath, taskNum, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileRedirectToResourceWithConnection downloads a file from resource to local in parallel
func (fs *FileSystem) DownloadFileRedirectToResourceWithConnection(controlConn *connection.IRODSConnection, irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %q: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if entry.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %q", irodsSrcPath)
	}

	stat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if stat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectFromResourceServerWithConnection(fs.ioSession, controlConn, entry.ToDataObject(), resource, localFilePath, taskNum, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	stat, err = os.Stat(localFilePath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to get stat of %q: %w", localFilePath, err)
	}

	fileTransferResult.LocalSize = stat.Size()

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(entry.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObject(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileWithConnection uploads a local file to irods
func (fs *FileSystem) UploadFileWithConnection(conn *connection.IRODSConnection, localPath string, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectWithConnection(conn, localSrcPath, irodsFilePath, resource, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileFromBuffer uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBuffer(buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			return fileTransferResult, xerrors.Errorf("invalid entry type %q. Destination must be a file", entry.Type)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if int64(buffer.Len()) < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = int64(buffer.Len())
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateBufferHash(buffer, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectFromBuffer(fs.ioSession, buffer, irodsFilePath, resource, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileFromBufferWithConnection uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBufferWithConnection(conn *connection.IRODSConnection, buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.StartTime = time.Now()

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			return fileTransferResult, xerrors.Errorf("invalid entry type %q. Destination must be a file", entry.Type)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if int64(buffer.Len()) < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = int64(buffer.Len())
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateBufferHash(buffer, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectFromBufferWithConnection(conn, buffer, irodsFilePath, resource, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileParallelWithConnections uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallelWithConnections(conns []*connection.IRODSConnection, localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectParallelWithConnections(conns, localSrcPath, irodsFilePath, resource, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileRedirectToResource uploads a file from local to resource server in parallel
func (fs *FileSystem) UploadFileRedirectToResource(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectToResourceServer(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileRedirectToResourceWithConnection uploads a file from local to resource server in parallel
func (fs *FileSystem) UploadFileRedirectToResourceWithConnection(controlConn *connection.IRODSConnection, localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, ignoreOverwriteError bool, transferCallback common.TransferTrackerCallback) (*FileTransferResult, error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.LocalPath = localSrcPath
	fileTransferResult.StartTime = time.Now()

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %q, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		if entry.IsDir() {
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		} else {
			if fs.IsTicketAccess() {
				// ticket does not support removing a file
				if stat.Size() < entry.Size {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to overwrite a file %q with a smaller file", irodsDestPath)
					}
				}

				// try to overwrite the file
			} else {
				err = fs.RemoveFile(irodsDestPath, true)
				if err != nil {
					if !ignoreOverwriteError {
						return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
					}
				}
			}
		}
	}

	fileTransferResult.LocalSize = stat.Size()
	fileTransferResult.IRODSPath = irodsFilePath

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		alg := types.ChecksumAlgorithmUnknown
		if entry != nil && entry.CheckSumAlgorithm != types.ChecksumAlgorithmUnknown {
			alg = entry.CheckSumAlgorithm
		}

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg, transferCallback)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %q: %w", checksumAlgorithm, err)
		}

		fileTransferResult.LocalCheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectToResourceServerWithConnection(fs.ioSession, controlConn, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, transferCallback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hash, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm, transferCallback)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hash

			if !bytes.Equal(entry.CheckSum, hash) {
				return fileTransferResult, xerrors.Errorf("checksum verification failed, upload failed")
			}
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// calculateLocalFileHash calculates local file hash
func (fs *FileSystem) calculateLocalFileHash(localPath string, algorithm types.ChecksumAlgorithm, processCallback common.TransferTrackerCallback) (types.ChecksumAlgorithm, []byte, error) {
	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	}

	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = defaultChecksumAlgorithm
	}

	hashCallback := func(name string, current int64, total int64) {
		if processCallback != nil {
			processCallback("checksum", current, total)
		}
	}

	// verify checksum
	hashBytes, err := util.HashLocalFile(localPath, string(algorithm), hashCallback)
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %q hash of %q: %w", algorithm, localPath, err)
	}

	return algorithm, hashBytes, nil
}

// calculateBufferHash calculates buffer hash
func (fs *FileSystem) calculateBufferHash(buffer *bytes.Buffer, algorithm types.ChecksumAlgorithm, processCallback common.TransferTrackerCallback) (types.ChecksumAlgorithm, []byte, error) {
	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	}

	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = defaultChecksumAlgorithm
	}

	hashCallback := func(name string, current int64, total int64) {
		if processCallback != nil {
			processCallback("checksum", current, total)
		}
	}

	// verify checksum
	hashBytes, err := util.HashBuffer(buffer, string(algorithm), hashCallback)
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %q hash of buffer data: %w", algorithm, err)
	}

	return algorithm, hashBytes, nil
}
