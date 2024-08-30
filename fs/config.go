package fs

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/session"
)

const (
	// FileSystemConnectionCreationTimeoutDefault is a default timeout value of connection error
	FileSystemConnectionCreationTimeoutDefault = 1 * time.Minute
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

type ConnectionConfig struct {
	CreationTimeout  time.Duration // timeout for creating a new connection
	InitNumber       int           // number of connections created when init
	MaxNumber        int           // max number of connections
	Lifespan         time.Duration // connection's lifespan (max time to be reused)
	OperationTimeout time.Duration // timeout for iRODS operations
	IdleTimeout      time.Duration // time out for being idle, after this point the connection will be disposed
	TCPBufferSize    int           // buffer size
}

func NewDefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:  FileSystemConnectionCreationTimeoutDefault,
		InitNumber:       FileSystemConnectionInitNumberDefault,
		MaxNumber:        FileSystemConnectionMaxDefault,
		Lifespan:         FileSystemConnectionLifespanDefault,
		OperationTimeout: FileSystemTimeoutDefault,
		IdleTimeout:      FileSystemTimeoutDefault,
		TCPBufferSize:    FileSystemTCPBufferSizeDefault,
	}
}

type CacheConfig struct {
	Timeout                 time.Duration // cache timeout
	CleanupTime             time.Duration //
	MetadataTimeoutSettings []MetadataCacheTimeoutSetting
	// determine if we will invalidate parent dir's entry cache
	// at subdir/file creation/deletion
	// turn to false to allow short cache inconsistency
	InvalidateParentEntryCacheImmediately bool
	// for mysql iCAT backend, this should be true.
	// for postgresql iCAT backend, this can be false.
	StartNewTransaction bool
}

func NewDefaultCacheConfig() CacheConfig {
	return CacheConfig{
		Timeout:                               FileSystemTimeoutDefault,
		CleanupTime:                           FileSystemTimeoutDefault,
		MetadataTimeoutSettings:               []MetadataCacheTimeoutSetting{},
		InvalidateParentEntryCacheImmediately: true,
		StartNewTransaction:                   true,
	}
}

// FileSystemConfig is a struct for file system configuration
type FileSystemConfig struct {
	ApplicationName string

	Cache CacheConfig
	//ConnectionErrorTimeout time.Duration
	//ConnectionInitNumber   int
	//ConnectionLifespan     time.Duration
	//OperationTimeout      time.Duration
	//ConnectionIdleTimeout time.Duration
	//ConnectionMax         int
	//TCPBufferSize        int

	AddressResolver session.AddressResolver
}

// NewFileSystemConfig create a FileSystemConfig with a default settings
func NewFileSystemConfig(applicationName string) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName: applicationName,

		Cache: NewDefaultCacheConfig(),

		// defaults
		ConnectionErrorTimeout: FileSystemConnectionCreationTimeoutDefault,
		ConnectionInitNumber:   FileSystemConnectionInitNumberDefault,
		ConnectionLifespan:     FileSystemConnectionLifespanDefault,
		OperationTimeout:       FileSystemTimeoutDefault,
		ConnectionIdleTimeout:  FileSystemTimeoutDefault,
		ConnectionMax:          FileSystemConnectionMaxDefault,
		TCPBufferSize:          FileSystemTCPBufferSizeDefault,
		//CacheTimeout:                          FileSystemTimeoutDefault,
		//CacheTimeoutSettings:                  []MetadataCacheTimeoutSetting{},
		//CacheCleanupTime:                      FileSystemTimeoutDefault,
		//StartNewTransaction:                   true,
		//InvalidateParentEntryCacheImmediately: true,
		AddressResolver: nil,
	}
}

// ToSessionConfig creates a IRODSSessionConfig from FileSystemConfig
func (config *FileSystemConfig) ToSessionConfig() *session.IRODSSessionConfig {
	sessionConfig := session.NewIRODSSessionConfig(config.ApplicationName)

	sessionConfig.ConnectionErrorTimeout = config.ConnectionErrorTimeout
	sessionConfig.ConnectionInitNumber = config.ConnectionInitNumber
	sessionConfig.ConnectionLifespan = config.ConnectionLifespan
	sessionConfig.OperationTimeout = config.OperationTimeout
	sessionConfig.ConnectionIdleTimeout = config.ConnectionIdleTimeout
	sessionConfig.ConnectionMax = config.ConnectionMax
	sessionConfig.TCPBufferSize = config.TCPBufferSize
	sessionConfig.ConnectionMaxIdle = config.ConnectionMax
	sessionConfig.StartNewTransaction = config.StartNewTransaction
	sessionConfig.AddressResolver = config.AddressResolver

	return sessionConfig
}
