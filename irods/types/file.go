package types

import "github.com/cyverse/go-irodsclient/irods/util"

// Whence determines where to start counting the offset
type Whence int

const (
	// SeekSet means offset starts from file start
	SeekSet Whence = 0
	// SeekCur means offset starts from current offset
	SeekCur Whence = 1
	// SeekEnd means offset starts from file end
	SeekEnd Whence = 2
)

// FileOpenMode determines file open mode
type FileOpenMode string

// FileOpenFlag is internally used value, derived from FileOpenMode
type FileOpenFlag int

const (
	// FileOpenModeReadOnly is for read
	FileOpenModeReadOnly FileOpenMode = "r"
	// FileOpenModeReadWrite is for read and write
	FileOpenModeReadWrite FileOpenMode = "r+"
	// FileOpenModeWriteOnly is for write
	FileOpenModeWriteOnly FileOpenMode = "w"
	// FileOpenModeWriteTruncate is for write, but truncates the file
	FileOpenModeWriteTruncate FileOpenMode = "w+"
	// FileOpenModeAppend is for write, not trucate, but appends from the file end
	FileOpenModeAppend FileOpenMode = "a"
	// FileOpenModeReadAppend is for read and write, but appends from the file end
	FileOpenModeReadAppend FileOpenMode = "a+"
)

const (
	// O_RDONLY is for read
	O_RDONLY FileOpenFlag = 0
	// O_WRONLY is for write
	O_WRONLY FileOpenFlag = 1
	// O_RDWR is for read and write
	O_RDWR FileOpenFlag = 2
	// O_APPEND is for append (moving the file pointer to the file end)
	O_APPEND FileOpenFlag = 1024
	// O_CREAT is for creating a file if not exists
	O_CREAT FileOpenFlag = 64
	// O_EXCL ...
	O_EXCL FileOpenFlag = 128
	// O_TRUNC is for truncating content
	O_TRUNC FileOpenFlag = 512
)

// GetFileOpenFlagSeekToEnd returns file open flag and returns true if file pointer moves to the file end
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

// IsFileOpenFlagRead returns true if the file open mode is for read
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

// IsFileOpenFlagWrite returns true if the file open mode is for write
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

// IsFileOpenFlagOpeningExisting returns true if the file open mode is for opening existing file
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
