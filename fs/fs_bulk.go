package fs

import (
	"fmt"
	"os"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// DownloadFile downloads a file to local
func (fs *FileSystem) DownloadFile(irodsPath string, resource string, localPath string) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	if srcStat.Type == DirectoryEntry {
		return fmt.Errorf("cannot download a collection %s", irodsSrcPath)
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
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			return fmt.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObject(fs.session, irodsSrcPath, resource, localFilePath)
}

// DownloadFileParallel downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallel(irodsPath string, resource string, localPath string, taskNum int) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	if srcStat.Type == DirectoryEntry {
		return fmt.Errorf("cannot download a collection %s", irodsSrcPath)
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
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			return fmt.Errorf("file %s already exists", localDestPath)
		}
	}

	return irods_fs.DownloadDataObjectParallel(fs.session, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum)
}

// DownloadFileParallelInBlocksAsync downloads a file to local in parallel
func (fs *FileSystem) DownloadFileParallelInBlocksAsync(irodsPath string, resource string, localPath string, blockLength int64, taskNum int) (chan int64, chan error) {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)
	localDestPath := util.GetCorrectIRODSPath(localPath)

	localFilePath := localDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		errChan <- types.NewFileNotFoundErrorf("could not find a data object")
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if srcStat.Type == DirectoryEntry {
		errChan <- fmt.Errorf("cannot download a collection %s", irodsSrcPath)
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
			localFilePath = util.MakeIRODSPath(localDestPath, irodsFileName)
		} else {
			errChan <- fmt.Errorf("file %s already exists", localDestPath)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	return irods_fs.DownloadDataObjectParallelInBlocksAsync(fs.session, irodsSrcPath, resource, localFilePath, srcStat.Size, blockLength, taskNum)
}

// UploadFile uploads a local file to irods
func (fs *FileSystem) UploadFile(localPath string, irodsPath string, resource string, replicate bool) error {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	stat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return types.NewFileNotFoundError("could not find the local file")
		}
		return err
	}

	if stat.IsDir() {
		return types.NewFileNotFoundError("The local file is a directory")
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
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fmt.Errorf("unknown entry type %s", entry.Type)
		}
	}

	err = irods_fs.UploadDataObject(fs.session, localSrcPath, irodsFilePath, resource, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool) error {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			return types.NewFileNotFoundError("could not find the local file")
		}
		return err
	}

	if srcStat.IsDir() {
		return types.NewFileNotFoundError("The local file is a directory")
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
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			return fmt.Errorf("unknown entry type %s", destStat.Type)
		}
	}

	err = irods_fs.UploadDataObjectParallel(fs.session, localSrcPath, irodsFilePath, resource, taskNum, replicate)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallelInBlocksAsync uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallelInBlocksAsync(localPath string, irodsPath string, resource string, blockLength int64, taskNum int, replicate bool) (chan int64, chan error) {
	localSrcPath := util.GetCorrectIRODSPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	outputChan := make(chan int64, 1)
	errChan := make(chan error, 1)

	srcStat, err := os.Stat(localSrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// file not exists
			errChan <- types.NewFileNotFoundError("could not find the local file")
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
		errChan <- types.NewFileNotFoundError("The local file is a directory")
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
			localFileName := util.GetIRODSPathFileName(localSrcPath)
			irodsFilePath = util.MakeIRODSPath(irodsDestPath, localFileName)
		default:
			errChan <- fmt.Errorf("unknown entry type %s", destStat.Type)
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}
	}

	outputChan2, errChan2 := irods_fs.UploadDataObjectParallelInBlockAsync(fs.session, localSrcPath, irodsFilePath, resource, blockLength, taskNum, replicate)

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return outputChan2, errChan2
}
