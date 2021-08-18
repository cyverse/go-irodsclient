package types

import "github.com/cyverse/go-irodsclient/irods/util"

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
		return int(O_WRONLY) | int(O_CREAT), false
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

// IsFileOpenFlagRead ...
func IsFileOpenFlagRead(mode FileOpenMode) bool {
	switch mode {
	case FileOpenModeReadOnly:
		return true
	case FileOpenModeReadWrite:
		return true
	case FileOpenModeWriteOnly:
		return false
	case FileOpenModeWriteTruncate:
		return false
	case FileOpenModeAppend:
		return false
	case FileOpenModeReadAppend:
		return true
	default:
		util.LogErrorf("Unhandled file open mode %s", mode)
		return false
	}
}

// IsFileOpenFlagWrite ...
func IsFileOpenFlagWrite(mode FileOpenMode) bool {
	switch mode {
	case FileOpenModeReadOnly:
		return false
	case FileOpenModeReadWrite:
		return true
	case FileOpenModeWriteOnly:
		return true
	case FileOpenModeWriteTruncate:
		return true
	case FileOpenModeAppend:
		return true
	case FileOpenModeReadAppend:
		return true
	default:
		util.LogErrorf("Unhandled file open mode %s", mode)
		return false
	}
}

// IsFileOpenFlagOpeningExisting ...
func IsFileOpenFlagOpeningExisting(mode FileOpenMode) bool {
	switch mode {
	case FileOpenModeReadOnly:
		return true
	case FileOpenModeReadWrite:
		return true
	case FileOpenModeWriteOnly:
		return false
	case FileOpenModeWriteTruncate:
		return false
	case FileOpenModeAppend:
		return true
	case FileOpenModeReadAppend:
		return true
	default:
		util.LogErrorf("Unhandled file open mode %s", mode)
		return false
	}
}
