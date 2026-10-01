package common

// constants
const (
	// VERSION
	IRODSVersionRelease string = "4.3.0"
	IRODSVersionAPI     string = "d"

	// Magic Numbers
	MaxQueryRows        int = 500
	MaxPasswordLength   int = 50
	MaxNameLength       int = 64
	ReadWriteBufferSize int = 1024 * 1024 * 4 // 4MB

	// MaxSizeForSingleBufferTransfer is the largest data object that is sent with the put
	// request itself, instead of setting up a transfer to a resource server.
	// It mirrors the reference client's default for max_size_for_single_buffer.
	MaxSizeForSingleBufferTransfer int64 = 1024 * 1024 * 32 // 32MB

	/*
		MAX_SQL_ATTR               int = 50
		MAX_PATH_ALLOWED           int = 1024
		MAX_NAME_LEN               int = MAX_PATH_ALLOWED + 64
		MAX_SQL_ROWS               int = 256
	*/
)
