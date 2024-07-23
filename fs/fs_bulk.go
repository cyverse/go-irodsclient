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
	CheckSumAlgorithm types.ChecksumAlgorithm
	IRODSPath         string
	IRODSCheckSum     []byte
	IRODSSize         int64
	LocalPath         string
	LocalCheckSum     []byte
	LocalSize         int64
	StartTime         time.Time
	EndTime           time.Time
}

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		localStat, err := os.Stat(localFilePath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get stat of %s: %w", localFilePath, err)
		}

		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = localStat.Size()

		if !bytes.Equal(srcStat.CheckSum, hash) {
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

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		localStat, err := os.Stat(localFilePath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get stat of %s: %w", localFilePath, err)
		}

		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = localStat.Size()

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return fileTransferResult, xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// DownloadFileToBuffer downloads a file to buffer
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer bytes.Buffer, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	fileTransferResult := &FileTransferResult{}
	fileTransferResult.IRODSPath = irodsSrcPath
	fileTransferResult.StartTime = time.Now()

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectToBuffer(fs.ioSession, irodsSrcPath, resource, buffer, srcStat.Size, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashBuffer(buffer, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = int64(buffer.Len())

		if !bytes.Equal(srcStat.CheckSum, hash) {
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

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallel(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		localStat, err := os.Stat(localFilePath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get stat of %s: %w", localFilePath, err)
		}

		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = localStat.Size()

		if !bytes.Equal(srcStat.CheckSum, hash) {
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

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return fileTransferResult, xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallelResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		localStat, err := os.Stat(localFilePath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get stat of %s: %w", localFilePath, err)
		}

		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = localStat.Size()

		if !bytes.Equal(srcStat.CheckSum, hash) {
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

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return fileTransferResult, xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return fileTransferResult, err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	fileTransferResult.LocalPath = localFilePath
	fileTransferResult.CheckSumAlgorithm = srcStat.CheckSumAlgorithm
	fileTransferResult.IRODSCheckSum = srcStat.CheckSum
	fileTransferResult.IRODSSize = srcStat.Size

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	checksumStr, err := irods_fs.DownloadDataObjectFromResourceServer(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return fileTransferResult, xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		localStat, err := os.Stat(localFilePath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get stat of %s: %w", localFilePath, err)
		}

		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		checksumBytes := srcStat.CheckSum
		if len(checksumStr) != 0 {
			checksumAlg, checksum, err := types.ParseIRODSChecksumString(checksumStr)
			if err != nil {
				return fileTransferResult, xerrors.Errorf("failed to parse checksum string of %s: %w", checksumStr, err)
			}

			checksumBytes = checksum

			if checksumAlg != srcStat.CheckSumAlgorithm {
				return fileTransferResult, xerrors.Errorf("checksum algorithm mismatch %s vs %s", checksumAlg, srcStat.CheckSumAlgorithm)
			}

			fileTransferResult.IRODSCheckSum = checksumBytes
		}

		fileTransferResult.LocalCheckSum = hash
		fileTransferResult.LocalSize = localStat.Size()

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
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fileTransferResult, xerrors.Errorf("unknown entry type %s", entry.Type)
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
		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %s: %w", checksumAlgorithm, err)
		}

		fileTransferResult.CheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObject(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	if verifyChecksum {
		// get stat again
		entry, err = fs.Stat(irodsFilePath)
		if err != nil {
			return fileTransferResult, err
		}

		fileTransferResult.CheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.IRODSCheckSum = entry.CheckSum
		fileTransferResult.IRODSSize = entry.Size
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// UploadFileFromBuffer uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBuffer(buffer bytes.Buffer, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) (*FileTransferResult, error) {
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
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			return fileTransferResult, xerrors.Errorf("invalid entry type %s. Destination must be a file", entry.Type)
		default:
			return fileTransferResult, xerrors.Errorf("unknown entry type %s", entry.Type)
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
		checksumAlgorithm, hashBytes, err := fs.calculateBufferHash(buffer)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %s: %w", checksumAlgorithm, err)
		}

		fileTransferResult.CheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectFromBuffer(fs.ioSession, buffer, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	if verifyChecksum {
		// get stat again
		entry, err = fs.Stat(irodsFilePath)
		if err != nil {
			return fileTransferResult, err
		}

		fileTransferResult.CheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.IRODSCheckSum = entry.CheckSum
		fileTransferResult.IRODSSize = entry.Size
	}

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
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Join(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fileTransferResult, xerrors.Errorf("unknown entry type %s", entry.Type)
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
		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %s: %w", checksumAlgorithm, err)
		}

		fileTransferResult.CheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	if verifyChecksum {
		// get stat again
		entry, err = fs.Stat(irodsFilePath)
		if err != nil {
			return fileTransferResult, err
		}

		fileTransferResult.CheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.IRODSCheckSum = entry.CheckSum
		fileTransferResult.IRODSSize = entry.Size
	}

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
			return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return fileTransferResult, err
	}

	if stat.IsDir() {
		return fileTransferResult, xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return fileTransferResult, err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Join(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fileTransferResult, xerrors.Errorf("unknown entry type %s", entry.Type)
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
		checksumAlgorithm, hashBytes, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		hashString, err := types.MakeIRODSChecksumString(checksumAlgorithm, hashBytes)
		if err != nil {
			return fileTransferResult, xerrors.Errorf("failed to get irods checksum string from algorithm %s: %w", checksumAlgorithm, err)
		}

		fileTransferResult.CheckSumAlgorithm = checksumAlgorithm
		fileTransferResult.LocalCheckSum = hashBytes

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectToResourceServer(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return fileTransferResult, err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	if verifyChecksum {
		// get stat again
		entry, err = fs.Stat(irodsFilePath)
		if err != nil {
			return fileTransferResult, err
		}

		fileTransferResult.CheckSumAlgorithm = entry.CheckSumAlgorithm
		fileTransferResult.IRODSCheckSum = entry.CheckSum
		fileTransferResult.IRODSSize = entry.Size
	}

	fileTransferResult.EndTime = time.Now()

	return fileTransferResult, nil
}

// calculateLocalFileHash calculates local file hash
func (fs *FileSystem) calculateLocalFileHash(localPath string) (types.ChecksumAlgorithm, []byte, error) {
	checksumAlg := types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	if checksumAlg == types.ChecksumAlgorithmUnknown {
		checksumAlg = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashLocalFile(localPath, string(checksumAlg))
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %s hash of %s: %w", checksumAlg, localPath, err)
	}

	return checksumAlg, hashBytes, nil
}

// calculateBufferHash calculates buffer hash
func (fs *FileSystem) calculateBufferHash(buffer bytes.Buffer) (types.ChecksumAlgorithm, []byte, error) {
	checksumAlg := types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	if checksumAlg == types.ChecksumAlgorithmUnknown {
		checksumAlg = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashBuffer(buffer, string(checksumAlg))
	if err != nil {
		return "", nil, xerrors.Errorf("failed to get %s hash of buffer data: %w", checksumAlg, err)
	}

	return checksumAlg, hashBytes, nil
}
