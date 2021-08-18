package fs

import "time"

const (
	FileSystemConnectionMaxMin     = 5
	FileSystemConnectionMaxDefault = 10
	FileSystemTimeoutDefault       = 5 * time.Minute
)

// FileSystemConfig ...
type FileSystemConfig struct {
	ApplicationName       string
	OperationTimeout      time.Duration
	ConnectionIdleTimeout time.Duration
	ConnectionMax         int
	CacheTimeout          time.Duration
	CacheCleanupTime      time.Duration
	// for mysql iCAT backend, this should be true.
	// for postgresql iCAT backend, this can be false.
	StartNewTransaction bool
}

// NewFileSystemConfig create a FileSystemConfig
func NewFileSystemConfig(applicationName string, operationTimeout time.Duration, connectionIdleTimeout time.Duration, connectionMax int, cacheTimeout time.Duration, cacheCleanupTime time.Duration, startNewTransaction bool) *FileSystemConfig {
	connMax := connectionMax
	if connMax < FileSystemConnectionMaxMin {
		connMax = FileSystemConnectionMaxMin
	}

	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: connectionIdleTimeout,
		ConnectionMax:         connMax,
		CacheTimeout:          cacheTimeout,
		CacheCleanupTime:      cacheCleanupTime,
		StartNewTransaction:   startNewTransaction,
	}
}

// NewFileSystemConfigWithDefault create a FileSystemConfig with a default settings
func NewFileSystemConfigWithDefault(applicationName string) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      FileSystemTimeoutDefault,
		ConnectionIdleTimeout: FileSystemTimeoutDefault,
		ConnectionMax:         FileSystemConnectionMaxDefault,
		CacheTimeout:          FileSystemTimeoutDefault,
		CacheCleanupTime:      FileSystemTimeoutDefault,
		StartNewTransaction:   true,
	}
}
