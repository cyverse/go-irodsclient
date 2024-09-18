package types

import (
	log "github.com/sirupsen/logrus"
)

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

//nolint:golint
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

// GetFlag returns file open flag
func (mode FileOpenMode) GetFlag() int {
	flag, _ := mode.GetFlagSeekToEnd()
	return flag
}

// Truncate returns if the mode needs truncating the file
func (mode FileOpenMode) Truncate() bool {
	switch mode {
	case FileOpenModeReadOnly:
		return false
	case FileOpenModeReadWrite:
		return false
	case FileOpenModeWriteOnly:
		return false
	case FileOpenModeWriteTruncate:
		return true
	case FileOpenModeAppend:
		return false
	case FileOpenModeReadAppend:
		return false
	default:
		return false
	}
}

// SeekToEnd returns if the mode needs seeking to end
func (mode FileOpenMode) SeekToEnd() bool {
	_, seekToEnd := mode.GetFlagSeekToEnd()
	return seekToEnd
}

// GetFlagSeekToEnd returns file open flag and returns true if file pointer moves to the file end
func (mode FileOpenMode) GetFlagSeekToEnd() (int, bool) {
	logger := log.WithFields(log.Fields{
		"package":  "types",
		"function": "GetFileOpenFlagSeekToEnd",
	})

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
		logger.Errorf("Unhandled file open mode %q", mode)
		return -1, false
	}
}

// IsRead returns true if the file open mode is for read
func (mode FileOpenMode) IsRead() bool {
	switch mode {
	case FileOpenModeReadOnly, FileOpenModeReadWrite, FileOpenModeReadAppend:
		return true
	default:
		return false
	}
}

// IsReadOnly returns true if the file open mode is for read-only
func (mode FileOpenMode) IsReadOnly() bool {
	return mode == FileOpenModeReadOnly
}

// IsWrite returns true if the file open mode is for write
func (mode FileOpenMode) IsWrite() bool {
	switch mode {
	case FileOpenModeReadWrite, FileOpenModeWriteOnly, FileOpenModeWriteTruncate, FileOpenModeAppend, FileOpenModeReadAppend:
		return true
	default:
		return false
	}
}

// IsWriteOnly returns true if the file open mode is for write-only
func (mode FileOpenMode) IsWriteOnly() bool {
	switch mode {
	case FileOpenModeWriteOnly, FileOpenModeWriteTruncate, FileOpenModeAppend:
		return true
	default:
		return false
	}
}

// IsOpeningExisting returns true if the file open mode is for opening existing file
func (mode FileOpenMode) IsOpeningExisting() bool {
	switch mode {
	case FileOpenModeReadOnly, FileOpenModeReadWrite, FileOpenModeReadAppend, FileOpenModeAppend:
		return true
	default:
		return false
	}
}
