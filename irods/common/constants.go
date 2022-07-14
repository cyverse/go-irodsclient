package common

// constants
const (
	// VERSION
	IRODSVersionRelease string = "4.3.0"
	IRODSVersionAPI     string = "d"

	// Magic Numbers
	MaxQueryRows        int = 500
	MaxPasswordLength   int = 50
	ReadWriteBufferSize int = 1024 * 1024 * 8 // 8MB

	/*
		MAX_SQL_ATTR               int = 50
		MAX_PATH_ALLOWED           int = 1024
		MAX_NAME_LEN               int = MAX_PATH_ALLOWED + 64
		MAX_SQL_ROWS               int = 256
	*/
)
