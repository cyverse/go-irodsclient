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
}

// NewFileSystemConfig create a FileSystemConfig
func NewFileSystemConfig(applicationName string, operationTimeout time.Duration, connectionIdleTimeout time.Duration, connectionMax int, cacheTimeout time.Duration, cacheCleanupTime time.Duration) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: connectionIdleTimeout,
		ConnectionMax:         connectionMax,
		CacheTimeout:          cacheTimeout,
		CacheCleanupTime:      cacheCleanupTime,
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
	}
}
