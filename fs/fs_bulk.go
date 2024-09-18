package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
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
	IRODSCheckSumAlgorithm types.ChecksumAlgorithm
	IRODSPath              string
	IRODSCheckSum          []byte
	IRODSSize              int64
	LocalCheckSumAlgorithm types.ChecksumAlgorithm
	LocalPath              string
	LocalCheckSum          []byte
	LocalSize              int64
	StartTime              time.Time
	EndTime                time.Time
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	err = irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, entry.Size, keywords, callback)
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
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm)
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
func (fs *FileSystem) DownloadFileResumable(irodsPath string, resource string, localPath string, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	err = irods_fs.DownloadDataObjectResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, entry.Size, keywords, callback)
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
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm)
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
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer *bytes.Buffer, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	err = irods_fs.DownloadDataObjectToBuffer(fs.ioSession, irodsSrcPath, resource, buffer, entry.Size, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %q: %w", irodsSrcPath, err)
	}

	fileTransferResult.LocalSize = int64(buffer.Len())

	if verifyChecksum {
		// verify checksum
		_, hash, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm)
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
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	err = irods_fs.DownloadDataObjectParallel(fs.ioSession, irodsSrcPath, resource, localFilePath, entry.Size, taskNum, keywords, callback)
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
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm)
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
func (fs *FileSystem) DownloadFileParallelResumable(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	err = irods_fs.DownloadDataObjectParallelResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, entry.Size, taskNum, keywords, callback)
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
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm)
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
func (fs *FileSystem) DownloadFileRedirectToResource(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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

	checksumStr, err := irods_fs.DownloadDataObjectFromResourceServer(fs.ioSession, irodsSrcPath, resource, localFilePath, entry.Size, taskNum, keywords, callback)
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
		_, hash, err := fs.calculateLocalFileHash(localFilePath, entry.CheckSumAlgorithm)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localFilePath, err)
		}

		checksumBytes := entry.CheckSum
		if len(checksumStr) != 0 {
			checksumAlg, checksum, err := types.ParseIRODSChecksumString(checksumStr)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to parse checksum string of %q: %w", checksumStr, err)
			}

			checksumBytes = checksum

			if checksumAlg != entry.CheckSumAlgorithm {
				return fileTransferResult, xerrors.Errorf("checksum algorithm mismatch %q vs %q", checksumAlg, entry.CheckSumAlgorithm)
			}

			fileTransferResult.IRODSCheckSum = checksumBytes
		}

		fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.LocalCheckSum = hash

		if !bytes.Equal(checksumBytes, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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
			err = fs.RemoveFile(irodsDestPath, true)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
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

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg)
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

	err = irods_fs.UploadDataObject(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hashBytes
		}
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size
	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileFromBuffer uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBuffer(buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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
			err = fs.RemoveFile(irodsDestPath, true)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
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

		checksumAlgorithm, hashBytes, err := fs.calculateBufferHash(buffer, alg)
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

	err = irods_fs.UploadDataObjectFromBuffer(fs.ioSession, buffer, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hashBytes, err := fs.calculateBufferHash(buffer, entry.CheckSumAlgorithm)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hashBytes
		}
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size
	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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
			err = fs.RemoveFile(irodsDestPath, true)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
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

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg)
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

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hashBytes
		}
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size
	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileParallelRedirectToResource uploads a file from local to resource server in parallel
func (fs *FileSystem) UploadFileParallelRedirectToResource(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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
			err = fs.RemoveFile(irodsDestPath, true)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to remove data object %q for overwrite: %w", irodsDestPath, err)
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

		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, alg)
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

	err = irods_fs.UploadDataObjectToResourceServer(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	entry, err = fs.Stat(irodsFilePath)
	if err != nil {
		return fileTransferResult, err
	}

	if verifyChecksum {
		if len(fileTransferResult.LocalCheckSumAlgorithm) > 0 && fileTransferResult.LocalCheckSumAlgorithm != entry.CheckSumAlgorithm {
			// different algorithm was used
			_, hashBytes, err := fs.calculateLocalFileHash(localSrcPath, entry.CheckSumAlgorithm)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to get hash of %q: %w", localSrcPath, err)
			}

			fileTransferResult.LocalCheckSumAlgorithm = entry.CheckSumAlgorithm
			fileTransferResult.LocalCheckSum = hashBytes
		}
	}

	fileTransferResult.IRODSCheckSumAlgorithm = entry.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = entry.CheckSum
	fileTransferResult.IRODSSize = entry.Size
	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// calculateLocalFileHash calculates local file hash
func (fs *FileSystem) calculateLocalFileHash(localPath string, algorithm types.ChecksumAlgorithm) (types.ChecksumAlgorithm, []byte, error) {
	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	}

	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashLocalFile(localPath, string(algorithm))
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %q hash of %q: %w", algorithm, localPath, err)
	}

	return algorithm, hashBytes, nil
}

// calculateBufferHash calculates buffer hash
func (fs *FileSystem) calculateBufferHash(buffer *bytes.Buffer, algorithm types.ChecksumAlgorithm) (types.ChecksumAlgorithm, []byte, error) {
	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	}

	if algorithm == types.ChecksumAlgorithmUnknown {
		algorithm = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashBuffer(buffer, string(algorithm))
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %q hash of buffer data: %w", algorithm, err)
	}

	return algorithm, hashBytes, nil
}
