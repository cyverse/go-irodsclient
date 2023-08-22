package types

// DataObjectLockType is a type for data object lock type
type DataObjectLockType string

const (
	// DataObjectLockTypeRead is for read lock
	DataObjectLockTypeRead DataObjectLockType = "readLockType"
	// DataObjectLockTypeWrite is for write lock
	DataObjectLockTypeWrite DataObjectLockType = "writeLockType"
	// DataObjectLockTypeWrite is for write lock
	DataObjectLockTypeUnlock DataObjectLockType = "unlockType"
)

// GetFileOpenMode returns FileOpenMode
func (t DataObjectLockType) GetFileOpenMode() FileOpenMode {
	switch t {
	case DataObjectLockTypeRead:
		return FileOpenModeReadOnly
	case DataObjectLockTypeWrite:
		return FileOpenModeWriteOnly
	default:
		return FileOpenModeReadOnly
	}
}

// DataObjectLockCommand is a type for data object lock command
type DataObjectLockCommand string

const (
	// DataObjectLockCommandSetLock is for set lock command
	DataObjectLockCommandSetLock DataObjectLockCommand = "setLockCmd"
	// DataObjectLockCommandSetLockWait is for set lock wait command
	DataObjectLockCommandSetLockWait DataObjectLockCommand = "setLockWaitCmd"
	// DataObjectLockCommandGetLock is for get lock command
	DataObjectLockCommandGetLock DataObjectLockCommand = "getLockCmd"
)
