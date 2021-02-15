package fs

import "time"

// FileSystemConfig ...
type FileSystemConfig struct {
	ApplicationName       string
	OperationTimeout      time.Duration
	ConnectionIdleTimeout time.Duration
	ConnectionMax         int
	CacheTimeout          time.Duration
}

// NewFileSystemConfig create a FileSystemConfig
func NewFileSystemConfig(applicationName string, operationTimeout time.Duration, connectionIdleTimeout time.Duration, connectionMax int, cacheTimeout time.Duration) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName:       applicationName,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: connectionIdleTimeout,
		ConnectionMax:         connectionMax,
		CacheTimeout:          cacheTimeout,
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
	}
}
