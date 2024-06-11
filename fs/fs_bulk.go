package fs

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/irods/common"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

const (
	defaultChecksumAlgorithm types.ChecksumAlgorithm = types.ChecksumAlgorithmMD5
)

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Lstat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// DownloadFileResumable downloads a file to local with support of transfer resume
func (fs *FileSystem) DownloadFileResumable(irodsPath string, resource string, localPath string, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Lstat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// DownloadFileToBuffer downloads a file to buffer
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer bytes.Buffer, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectToBuffer(fs.ioSession, irodsSrcPath, resource, buffer, srcStat.Size, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashBuffer(buffer, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// DownloadFileParallel downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Lstat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallel(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// DownloadFileParallelResumable downloads a file to local in parallel with support of transfer resume
func (fs *FileSystem) DownloadFileParallelResumable(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Lstat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	if verifyChecksum {
		// verify checksum
		if len(srcStat.CheckSum) == 0 {
			return xerrors.Errorf("failed to get checksum of the source data object for path %s", irodsSrcPath)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	err = irods_fs.DownloadDataObjectParallelResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		if !bytes.Equal(srcStat.CheckSum, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// DownloadFileRedirectToResource downloads a file from resource to local in parallel
func (fs *FileSystem) DownloadFileRedirectToResource(irodsPath string, resource string, localPath string, taskNum int, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Lstat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			return err
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		}
	}

	keywords := map[common.KeyWord]string{}
	if verifyChecksum {
		keywords[common.VERIFY_CHKSUM_KW] = ""
	}

	checksumStr, err := irods_fs.DownloadDataObjectFromResourceServer(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, keywords, callback)
	if err != nil {
		return xerrors.Errorf("failed to download a data object for path %s: %w", irodsSrcPath, err)
	}

	if verifyChecksum {
		// verify checksum
		hash, err := util.HashLocalFile(localFilePath, string(srcStat.CheckSumAlgorithm))
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localFilePath, err)
		}

		checksumBytes := srcStat.CheckSum
		if len(checksumStr) != 0 {
			checksumAlg, checksum, err := types.ParseIRODSChecksumString(checksumStr)
			if err != nil {
				return xerrors.Errorf("failed to parse checksum string of %s: %w", checksumStr, err)
			}

			checksumBytes = checksum

			if checksumAlg != srcStat.CheckSumAlgorithm {
				return xerrors.Errorf("checksum algorithm mismatch %s vs %s", checksumAlg, srcStat.CheckSumAlgorithm)
			}
		}

		if !bytes.Equal(checksumBytes, hash) {
			return xerrors.Errorf("checksum verification failed, download failed")
		}
	}

	return nil
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	stat, err := os.Lstat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return err
	}

	if stat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return xerrors.Errorf("unknown entry type %s", entry.Type)
		}
	}

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		hashString, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObject(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	return nil
}

// UploadFileFromBuffer uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBuffer(buffer bytes.Buffer, irodsPath string, resource string, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) error {
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	entry, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch entry.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			return xerrors.Errorf("invalid entry type %s. Destination must be a file", entry.Type)
		default:
			return xerrors.Errorf("unknown entry type %s", entry.Type)
		}
	}

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		hashString, err := fs.calculateBufferHash(buffer)
		if err != nil {
			return xerrors.Errorf("failed to get hash of buffer data: %w", err)
		}

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectFromBuffer(fs.ioSession, buffer, irodsFilePath, resource, replicate, keywords, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	return nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Lstat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return err
	}

	if srcStat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	destStat, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch destStat.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Join(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return xerrors.Errorf("unknown entry type %s", destStat.Type)
		}
	}

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		hashString, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	return nil
}

// UploadFileParallelRedirectToResource uploads a file from local to resource server in parallel
func (fs *FileSystem) UploadFileParallelRedirectToResource(localPath string, irodsPath string, resource string, taskNum int, replicate bool, checksum bool, verifyChecksum bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Lstat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to find a file for local path %s: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
		}
		return err
	}

	if srcStat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError(localSrcPath))
	}

	destStat, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			return err
		}
	} else {
		switch destStat.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Join(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return xerrors.Errorf("unknown entry type %s", destStat.Type)
		}
	}

	keywords := map[common.KeyWord]string{}
	if checksum {
		keywords[common.REG_CHKSUM_KW] = ""
	}

	if verifyChecksum {
		// verify checksum
		hashString, err := fs.calculateLocalFileHash(localSrcPath)
		if err != nil {
			return xerrors.Errorf("failed to get hash of %s: %w", localSrcPath, err)
		}

		keywords[common.VERIFY_CHKSUM_KW] = hashString
	}

	err = irods_fs.UploadDataObjectToResourceServer(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, keywords, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)

	return nil
}

// calculateLocalFileHash calculates local file hash
func (fs *FileSystem) calculateLocalFileHash(localPath string) (string, error) {
	checksumAlg := types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	if checksumAlg == types.ChecksumAlgorithmUnknown {
		checksumAlg = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashLocalFile(localPath, string(checksumAlg))
	if err != nil {
		return "", xerrors.Errorf("failed to get %s hash of %s: %w", checksumAlg, localPath, err)
	}

	return types.MakeIRODSChecksumString(checksumAlg, hashBytes)
}

// calculateBufferHash calculates buffer hash
func (fs *FileSystem) calculateBufferHash(buffer bytes.Buffer) (string, error) {
	checksumAlg := types.GetChecksumAlgorithm(fs.account.DefaultHashScheme)
	if checksumAlg == types.ChecksumAlgorithmUnknown {
		checksumAlg = defaultChecksumAlgorithm
	}

	// verify checksum
	hashBytes, err := util.HashBuffer(buffer, string(checksumAlg))
	if err != nil {
		return "", xerrors.Errorf("failed to get %s hash of buffer data: %w", checksumAlg, err)
	}

	return types.MakeIRODSChecksumString(checksumAlg, hashBytes)
}
