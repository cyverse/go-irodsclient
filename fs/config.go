package fs

import "time"

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
	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: connectionIdleTimeout,
		ConnectionMax:         connectionMax,
		CacheTimeout:          cacheTimeout,
		CacheCleanupTime:      cacheCleanupTime,
		StartNewTransaction:   startNewTransaction,
	}
}

// NewFileSystemConfigWithDefault create a FileSystemConfig with a default settings
func NewFileSystemConfigWithDefault(applicationName string) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      5 * time.Minute,
		ConnectionIdleTimeout: 5 * time.Minute,
		ConnectionMax:         20,
		CacheTimeout:          5 * time.Minute,
		CacheCleanupTime:      5 * time.Minute,
		StartNewTransaction:   true,
	}
}
