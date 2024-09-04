package fs

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/session"
)

const (
	// FileSystemConnectionCreationTimeoutDefault is a default timeout value of connection error
	FileSystemConnectionCreationTimeoutDefault = 1 * time.Minute
	// FileSystemConnectionLifespanDefault is a default lifespan of a connection
	FileSystemConnectionLifespanDefault = 1 * time.Hour
	// FileSystemTimeoutDefault is a default value of timeout
	FileSystemTimeoutDefault = 5 * time.Minute
	// FileSystemTCPBufferSizeDefault is a default value of tcp buffer size
	FileSystemTCPBufferSizeDefault = 4 * 1024 * 1024

	// Metadata Connection
	// FileSystemMetadataConnectionInitNumberDefault is a default value of connection init number
	FileSystemMetadataConnectionInitNumberDefault = 1
	// FileSystemMetadataConnectionMaxNumberDefault is a default number of connection max value
	FileSystemMetadataConnectionMaxNumberDefault = 2
	// FileSystemMetadataConnectionMaxIdleNumberDefault is a default number of max idle connections
	FileSystemMetadataConnectionMaxIdleNumberDefault = 2

	// IO Connection
	// FileSystemIOConnectionInitNumberDefault is a default value of connection init number
	FileSystemIOConnectionInitNumberDefault = 0
	// FileSystemIOConnectionMaxNumberDefault is a default number of connection max value
	FileSystemIOConnectionMaxNumberDefault = 10
	// FileSystemIOConnectionMaxIdleNumberDefault is a default number of max idle connections
	FileSystemIOConnectionMaxIdleNumberDefault = 4
)

// ConnectionConfig is a struct that stores configuration for connections
type ConnectionConfig struct {
	CreationTimeout  time.Duration // timeout for creating a new connection
	InitNumber       int           // number of connections created when init
	MaxNumber        int           // max number of connections
	MaxIdleNumber    int           // max number of idle connections
	Lifespan         time.Duration // connection's lifespan (max time to be reused)
	OperationTimeout time.Duration // timeout for iRODS operations
	IdleTimeout      time.Duration // time out for being idle, after this point the connection will be disposed
	TCPBufferSize    int           // buffer size
}

// NewDefaultMetadataConnectionConfig creates a default ConnectionConfig for metadata
func NewDefaultMetadataConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:  FileSystemConnectionCreationTimeoutDefault,
		InitNumber:       FileSystemMetadataConnectionInitNumberDefault,
		MaxNumber:        FileSystemMetadataConnectionMaxNumberDefault,
		MaxIdleNumber:    FileSystemMetadataConnectionMaxIdleNumberDefault,
		Lifespan:         FileSystemConnectionLifespanDefault,
		OperationTimeout: FileSystemTimeoutDefault,
		IdleTimeout:      FileSystemTimeoutDefault,
		TCPBufferSize:    FileSystemTCPBufferSizeDefault,
	}
}

// NewDefaultIOConnectionConfig creates a default ConnectionConfig for IO
func NewDefaultIOConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:  FileSystemConnectionCreationTimeoutDefault,
		InitNumber:       FileSystemIOConnectionInitNumberDefault,
		MaxNumber:        FileSystemIOConnectionMaxNumberDefault,
		MaxIdleNumber:    FileSystemIOConnectionMaxIdleNumberDefault,
		Lifespan:         FileSystemConnectionLifespanDefault,
		OperationTimeout: FileSystemTimeoutDefault,
		IdleTimeout:      FileSystemTimeoutDefault,
		TCPBufferSize:    FileSystemTCPBufferSizeDefault,
	}
}

// FileSystemConfig is a struct for file system configuration
type FileSystemConfig struct {
	ApplicationName string

	MetadataConnection ConnectionConfig
	IOConnection       ConnectionConfig

	Cache CacheConfig

	AddressResolver session.AddressResolver
}

// NewFileSystemConfig create a FileSystemConfig with a default settings
func NewFileSystemConfig(applicationName string) *FileSystemConfig {
	return &FileSystemConfig{
		ApplicationName: applicationName,

		MetadataConnection: NewDefaultMetadataConnectionConfig(),
		IOConnection:       NewDefaultIOConnectionConfig(),
		Cache:              NewDefaultCacheConfig(),

		AddressResolver: nil,
	}
}

// ToMetadataSessionConfig creates a IRODSSessionConfig from FileSystemConfig
func (config *FileSystemConfig) ToMetadataSessionConfig() *session.IRODSSessionConfig {
	sessionConfig := session.NewIRODSSessionConfig(config.ApplicationName)

	sessionConfig.ConnectionCreationTimeout = config.MetadataConnection.CreationTimeout
	sessionConfig.ConnectionInitNumber = config.MetadataConnection.InitNumber
	sessionConfig.ConnectionLifespan = config.MetadataConnection.Lifespan
	sessionConfig.OperationTimeout = config.MetadataConnection.OperationTimeout
	sessionConfig.ConnectionIdleTimeout = config.MetadataConnection.IdleTimeout
	sessionConfig.ConnectionMaxNumber = config.MetadataConnection.MaxNumber
	sessionConfig.ConnectionMaxIdleNumber = config.MetadataConnection.MaxIdleNumber
	sessionConfig.TCPBufferSize = config.MetadataConnection.TCPBufferSize
	sessionConfig.StartNewTransaction = config.Cache.StartNewTransaction

	sessionConfig.AddressResolver = config.AddressResolver

	return sessionConfig
}

// ToIOSessionConfig creates a IRODSSessionConfig from FileSystemConfig
func (config *FileSystemConfig) ToIOSessionConfig() *session.IRODSSessionConfig {
	sessionConfig := session.NewIRODSSessionConfig(config.ApplicationName)

	sessionConfig.ConnectionCreationTimeout = config.IOConnection.CreationTimeout
	sessionConfig.ConnectionInitNumber = config.IOConnection.InitNumber
	sessionConfig.ConnectionLifespan = config.IOConnection.Lifespan
	sessionConfig.OperationTimeout = config.IOConnection.OperationTimeout
	sessionConfig.ConnectionIdleTimeout = config.IOConnection.IdleTimeout
	sessionConfig.ConnectionMaxNumber = config.IOConnection.MaxNumber
	sessionConfig.ConnectionMaxIdleNumber = config.IOConnection.MaxIdleNumber
	sessionConfig.TCPBufferSize = config.IOConnection.TCPBufferSize
	sessionConfig.StartNewTransaction = config.Cache.StartNewTransaction

	sessionConfig.AddressResolver = config.AddressResolver

	return sessionConfig
}
