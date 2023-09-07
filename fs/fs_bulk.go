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

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to stat for path %s: %w", irodsSrcPath, types.NewFileNotFoundError())
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
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
		} else {
			return xerrors.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, callback)
}

/*
// DownloadFileWithProgressFile downloads a file to local and creates an intermediate progress file
func (fs *FileSystem) DownloadFileWithProgressFile(irodsPath string, resource string, localPath string, progressFilePath string, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)
	localProgressFilePath := util.GetCorrectLocalPath(progressFilePath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to stat for path %s: %w", irodsSrcPath, types.NewFileNotFoundError())
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
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
		} else {
			return xerrors.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, callback)
}
*/

// DownloadFileToBuffer downloads a file to buffer
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer bytes.Buffer, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to stat for path %s: %w", irodsSrcPath, types.NewFileNotFoundError())
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	return irods_fs.DownloadDataObjectToBuffer(fs.ioSession, irodsSrcPath, resource, buffer, srcStat.Size, callback)
}

// DownloadFileParallel downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to stat for path %s: %w", irodsSrcPath, types.NewFileNotFoundError())
	}

	if srcStat.Type == DirectoryEntry {
		return xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
	}

	destStat, err := os.Stat(localDestPath)
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
		} else {
			return xerrors.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObjectParallel(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, callback)
}

// DownloadFileParallelInBlocksAsync downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallelInBlocksAsync(irodsPath string, resource string, localPath string, blockLength int64, taskNum int) (chan int64, chan error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectLocalPath(localPath)

	localFilePath := localDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		errChan <- xerrors.Errorf("failed to stat for path %s: %w", irodsSrcPath, types.NewFileNotFoundError())
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if srcStat.Type == DirectoryEntry {
		errChan <- xerrors.Errorf("cannot download a collection %s", irodsSrcPath)
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	destStat, err := os.Stat(localDestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists, it's a file
			// pass
		} else {
			errChan <- err
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	} else {
		if destStat.IsDir() {
			irodsFileName := util.GetIRODSPathFileName(irodsSrcPath)
			localFilePath = filepath.Join(localDestPath, irodsFileName)
		} else {
			errChan <- xerrors.Errorf("file %s already exists", localDestPath)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	return irods_fs.DownloadDataObjectParallelInBlocksAsync(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, blockLength, taskNum)
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to stat for local path %s: %w", localSrcPath, types.NewFileNotFoundError())
		}
		return err
	}

	if stat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError())
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

	err = irods_fs.UploadDataObject(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileFromBuffer uploads buffer data to irods
func (fs *FileSystem) UploadFileFromBuffer(buffer bytes.Buffer, irodsPath string, resource string, replicate bool, callback common.TrackerCallBack) error {
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

	err = irods_fs.UploadDataObjectFromBuffer(fs.ioSession, buffer, irodsFilePath, resource, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileAsync uploads a local file to irods
func (fs *FileSystem) UploadFileAsync(localPath string, irodsPath string, resource string, replicate bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to stat for local path %s: %w", localSrcPath, types.NewFileNotFoundError())
		}
		return err
	}

	if stat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError())
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

	err = irods_fs.UploadDataObjectAsync(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return xerrors.Errorf("failed to stat for local path %s: %w", localSrcPath, types.NewFileNotFoundError())
		}
		return err
	}

	if srcStat.IsDir() {
		return xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError())
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

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallelInBlocksAsync uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallelInBlocksAsync(localPath string, irodsPath string, resource string, blockLength int64, taskNum int, replicate bool) (chan int64, chan error) {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			errChan <- xerrors.Errorf("failed to stat for local path %s: %w", localSrcPath, types.NewFileNotFoundError())
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}

		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if srcStat.IsDir() {
		errChan <- xerrors.Errorf("failed to find a file for local path %s, the path is for a directory: %w", localSrcPath, types.NewFileNotFoundError())
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	destStat, err := fs.Stat(irodsDestPath)
	if err != nil {
		if !types.IsFileNotFoundError(err) {
			errChan <- err
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	} else {
		switch destStat.Type {
		case FileEntry:
			// do nothing
		case DirectoryEntry:
			localFileName := filepath.Base(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			errChan <- xerrors.Errorf("unknown entry type %s", destStat.Type)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	outputChan2, errChan2 := irods_fs.UploadDataObjectParallelInBlockAsync(fs.ioSession, localSrcPath, irodsFilePath, resource, blockLength, taskNum, replicate)

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return outputChan2, errChan2
}
