package types

import "github.com/iychoi/go-irodsclient/pkg/irods/util"

// Whence ...
type Whence int

const (
	SeekSet Whence = 0
	SeekCur Whence = 1
	SeekEnd Whence = 2
)

type FileOpenMode string
type FileOpenFlag int

const (
	FileOpenModeReadOnly      FileOpenMode = "r"
	FileOpenModeReadWrite     FileOpenMode = "r+"
	FileOpenModeWriteOnly     FileOpenMode = "w"
	FileOpenModeWriteTruncate FileOpenMode = "w+"
	FileOpenModeAppend        FileOpenMode = "a"
	FileOpenModeReadAppend    FileOpenMode = "a+"
)

const (
	O_RDONLY FileOpenFlag = 0
	O_WRONLY FileOpenFlag = 1
	O_RDWR   FileOpenFlag = 2
	O_APPEND FileOpenFlag = 1024
	O_CREAT  FileOpenFlag = 64
	O_EXCL   FileOpenFlag = 128
	O_TRUNC  FileOpenFlag = 512
)

// GetFileOpenFlagSeekToEnd ...
func GetFileOpenFlagSeekToEnd(mode FileOpenMode) (int, bool) {
	switch mode {
	case FileOpenModeReadOnly:
		return int(O_RDONLY), false
	case FileOpenModeReadWrite:
		return int(O_RDWR), false
	case FileOpenModeWriteOnly:
		return int(O_WRONLY) | int(O_CREAT) | int(O_TRUNC), false
	case FileOpenModeWriteTruncate:
		return int(O_RDWR) | int(O_CREAT) | int(O_TRUNC), false
	case FileOpenModeAppend:
		return int(O_WRONLY) | int(O_CREAT), true
	case FileOpenModeReadAppend:
		return int(O_RDWR) | int(O_CREAT), true
	default:
		util.LogErrorf("Unhandled file open mode %s", mode)
		return -1, false
	}
}
