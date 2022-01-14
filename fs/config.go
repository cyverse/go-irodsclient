package fs

import "time"

const (
	// FileSystemConnectionMaxMin is a minimum number of connection max value
	FileSystemConnectionMaxMin = 5
	// FileSystemConnectionMaxDefault is a default number of connection max value
	FileSystemConnectionMaxDefault = 10
	// ConnectionLifespanDefault is a default lifespan of a connection
	ConnectionLifespanDefault = 1 * time.Hour
	// FileSystemTimeoutDefault is a default timeout value
	FileSystemTimeoutDefault = 5 * time.Minute
)

// FileSystemConfig is a struct for file system configuration
type FileSystemConfig struct {
	ApplicationName       string
	ConnectionLifespan    time.Duration
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
func NewFileSystemConfig(applicationName string, connectionLifespan time.Duration, operationTimeout time.Duration, connectionIdleTimeout time.Duration, connectionMax int, cacheTimeout time.Duration, cacheCleanupTime time.Duration, startNewTransaction bool) *FileSystemConfig {
	connMax := connectionMax
	if connMax < FileSystemConnectionMaxMin {
		connMax = FileSystemConnectionMaxMin
	}

	return &FileSystemConfig{
		ApplicationName:       applicationName,
		ConnectionLifespan:    connectionLifespan,
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
		ConnectionLifespan:    ConnectionLifespanDefault,
		OperationTimeout:      FileSystemTimeoutDefault,
		ConnectionIdleTimeout: FileSystemTimeoutDefault,
		ConnectionMax:         FileSystemConnectionMaxDefault,
		CacheTimeout:          FileSystemTimeoutDefault,
		CacheCleanupTime:      FileSystemTimeoutDefault,
		StartNewTransaction:   true,
	}
}
