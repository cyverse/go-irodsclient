package fs

import "time"

const (
	// FileSystemConnectionErrorTimeoutDefault is a default timeout value of connection error
	FileSystemConnectionErrorTimeoutDefault = 1 * time.Minute
	// FileSystemConnectionInitNumberDefault is a default value of connection init number
	FileSystemConnectionInitNumberDefault = 0
	// FileSystemConnectionMaxMin is a minimum number of connection max value
	FileSystemConnectionMaxMin = 5
	// FileSystemConnectionMaxDefault is a default number of connection max value
	FileSystemConnectionMaxDefault = 10
	// FileSystemConnectionMetaDefault is a default number of metadata operation connection
	FileSystemConnectionMetaDefault = 2
	// FileSystemConnectionLifespanDefault is a default lifespan of a connection
	FileSystemConnectionLifespanDefault = 1 * time.Hour
	// FileSystemTimeoutDefault is a default value of timeout
	FileSystemTimeoutDefault = 5 * time.Minute
	// FileSystemTCPBufferSizeDefault is a default value of tcp buffer size
	FileSystemTCPBufferSizeDefault = 4 * 1024 * 1024
)

// FileSystemConfig is a struct for file system configuration
type FileSystemConfig struct {
	ApplicationName        string
	ConnectionErrorTimeout time.Duration
	ConnectionInitNumber   int
	ConnectionLifespan     time.Duration
	OperationTimeout       time.Duration
	ConnectionIdleTimeout  time.Duration
	ConnectionMax          int
	TcpBufferSize          int
	CacheTimeout           time.Duration
	CacheCleanupTime       time.Duration
	CacheTimeoutSettings   []MetadataCacheTimeoutSetting
	// for mysql iCAT backend, this should be true.
	// for postgresql iCAT backend, this can be false.
	StartNewTransaction bool
	// determine if we will invalidate parent dir's entry cache
	// at subdir/file creation/deletion
	// turn to false to allow short cache inconsistency
	InvalidateParentEntryCacheImmediately bool
}

// NewFileSystemConfig create a FileSystemConfig
func NewFileSystemConfig(applicationName string, connectionErrorTimeout time.Duration, connectionInitNumber int, connectionLifespan time.Duration, operationTimeout time.Duration, connectionIdleTimeout time.Duration, connectionMax int, tcpBufferSize int, cacheTimeout time.Duration, cacheCleanupTime time.Duration, cacheTimeoutSettings []MetadataCacheTimeoutSetting, startNewTransaction bool, invalidateParentEntryCacheImmediately bool) *FileSystemConfig {
	connMax := connectionMax
	if connMax < FileSystemConnectionMaxMin {
		connMax = FileSystemConnectionMaxMin
	}

	return &FileSystemConfig{
		ApplicationName:                       applicationName,
		ConnectionErrorTimeout:                connectionErrorTimeout,
		ConnectionInitNumber:                  connectionInitNumber,
		ConnectionLifespan:                    connectionLifespan,
		OperationTimeout:                      operationTimeout,
		ConnectionIdleTimeout:                 connectionIdleTimeout,
		ConnectionMax:                         connMax,
		TcpBufferSize:                         tcpBufferSize,
		CacheTimeout:                          cacheTimeout,
		CacheCleanupTime:                      cacheCleanupTime,
		CacheTimeoutSettings:                  cacheTimeoutSettings,
		StartNewTransaction:                   startNewTransaction,
		InvalidateParentEntryCacheImmediately: invalidateParentEntryCacheImmediately,
	}
}

// NewFileSystemConfigWithDefault create a FileSystemConfig with a default settings
func NewFileSystemConfigWithDefault(applicationName string) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName:                       applicationName,
		ConnectionErrorTimeout:                FileSystemConnectionErrorTimeoutDefault,
		ConnectionInitNumber:                  FileSystemConnectionInitNumberDefault,
		ConnectionLifespan:                    FileSystemConnectionLifespanDefault,
		OperationTimeout:                      FileSystemTimeoutDefault,
		ConnectionIdleTimeout:                 FileSystemTimeoutDefault,
		ConnectionMax:                         FileSystemConnectionMaxDefault,
		TcpBufferSize:                         FileSystemTCPBufferSizeDefault,
		CacheTimeout:                          FileSystemTimeoutDefault,
		CacheTimeoutSettings:                  []MetadataCacheTimeoutSetting{},
		CacheCleanupTime:                      FileSystemTimeoutDefault,
		StartNewTransaction:                   true,
		InvalidateParentEntryCacheImmediately: true,
	}
}
