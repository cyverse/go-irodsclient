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
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
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
		}
	}

	return irods_fs.DownloadDataObject(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, callback)
}

// DownloadFileResumable downloads a file to local with support of transfer resume
func (fs *FileSystem) DownloadFileResumable(irodsPath string, resource string, localPath string, callback common.TrackerCallBack) error {
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
		}
	}

	return irods_fs.DownloadDataObjectResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, callback)
}

// DownloadFileToBuffer downloads a file to buffer
func (fs *FileSystem) DownloadFileToBuffer(irodsPath string, resource string, buffer bytes.Buffer, callback common.TrackerCallBack) error {
	irodsSrcPath := util.GetCorrectIRODSPath(irodsPath)

	srcStat, err := fs.Stat(irodsSrcPath)
	if err != nil {
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
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
		return xerrors.Errorf("failed to find a data object for path %s: %w", irodsSrcPath, types.NewFileNotFoundError(irodsSrcPath))
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
		}
	}

	return irods_fs.DownloadDataObjectParallel(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, callback)
}

// DownloadFileParallelResumable downloads a file to local in parallel with support of transfer resume
func (fs *FileSystem) DownloadFileParallelResumable(irodsPath string, resource string, localPath string, taskNum int, callback common.TrackerCallBack) error {
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
		}
	}

	return irods_fs.DownloadDataObjectParallelResumable(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, taskNum, callback)
}

// DownloadFileRedirectToResource downloads a file from resource to local in parallel
func (fs *FileSystem) DownloadFileRedirectToResource(irodsPath string, resource string, localPath string, callback common.TrackerCallBack) error {
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
		}
	}

	return irods_fs.DownloadDataObjectFromResourceServer(fs.ioSession, irodsSrcPath, resource, localFilePath, srcStat.Size, callback)
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

// UploadFileParallel uploads a local file to irods in parallel
func (fs *FileSystem) UploadFileParallel(localPath string, irodsPath string, resource string, taskNum int, replicate bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Stat(localSrcPath)
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

	err = irods_fs.UploadDataObjectParallel(fs.ioSession, localSrcPath, irodsFilePath, resource, taskNum, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}

// UploadFileParallelRedirectToResource uploads a file from local to resource server in parallel
func (fs *FileSystem) UploadFileParallelRedirectToResource(localPath string, irodsPath string, resource string, replicate bool, callback common.TrackerCallBack) error {
	localSrcPath := util.GetCorrectLocalPath(localPath)
	irodsDestPath := util.GetCorrectIRODSPath(irodsPath)

	irodsFilePath := irodsDestPath

	srcStat, err := os.Stat(localSrcPath)
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

	err = irods_fs.UploadDataObjectToResourceServer(fs.ioSession, localSrcPath, irodsFilePath, resource, replicate, callback)
	if err != nil {
		return err
	}

	fs.invalidateCacheForFileCreate(irodsFilePath)
	fs.cachePropagation.PropagateFileCreate(irodsFilePath)
	return nil
}
