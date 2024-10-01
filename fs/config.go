package fs

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
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
	CreationTimeout  types.Duration `yaml:"creation_timeout,omitempty" json:"creation_timeout,omitempty"`   // timeout for creating a new connection
	InitNumber       int            `yaml:"init_number,omitempty" json:"init_number,omitempty"`             // number of connections created when init
	MaxNumber        int            `yaml:"max_number,omitempty" json:"max_number,omitempty"`               // max number of connections
	MaxIdleNumber    int            `yaml:"max_idle_number,omitempty" json:"max_idle_number,omitempty"`     // max number of idle connections
	Lifespan         types.Duration `yaml:"lifespan,omitempty" json:"lifespan,omitempty"`                   // connection's lifespan (max time to be reused)
	OperationTimeout types.Duration `yaml:"operation_timeout,omitempty" json:"operation_timeout,omitempty"` // timeout for iRODS operations
	IdleTimeout      types.Duration `yaml:"idle_timeout,omitempty" json:"idle_timeout,omitempty"`           // time out for being idle, after this point the connection will be disposed
	TCPBufferSize    int            `yaml:"tcp_buffer_size,omitempty" json:"tcp_buffer_size,omitempty"`     // buffer size
}

// NewDefaultMetadataConnectionConfig creates a default ConnectionConfig for metadata
func NewDefaultMetadataConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:  types.Duration(FileSystemConnectionCreationTimeoutDefault),
		InitNumber:       FileSystemMetadataConnectionInitNumberDefault,
		MaxNumber:        FileSystemMetadataConnectionMaxNumberDefault,
		MaxIdleNumber:    FileSystemMetadataConnectionMaxIdleNumberDefault,
		Lifespan:         types.Duration(FileSystemConnectionLifespanDefault),
		OperationTimeout: types.Duration(FileSystemTimeoutDefault),
		IdleTimeout:      types.Duration(FileSystemTimeoutDefault),
		TCPBufferSize:    FileSystemTCPBufferSizeDefault,
	}
}

// NewDefaultIOConnectionConfig creates a default ConnectionConfig for IO
func NewDefaultIOConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:  types.Duration(FileSystemConnectionCreationTimeoutDefault),
		InitNumber:       FileSystemIOConnectionInitNumberDefault,
		MaxNumber:        FileSystemIOConnectionMaxNumberDefault,
		MaxIdleNumber:    FileSystemIOConnectionMaxIdleNumberDefault,
		Lifespan:         types.Duration(FileSystemConnectionLifespanDefault),
		OperationTimeout: types.Duration(FileSystemTimeoutDefault),
		IdleTimeout:      types.Duration(FileSystemTimeoutDefault),
		TCPBufferSize:    FileSystemTCPBufferSizeDefault,
	}
}

// FileSystemConfig is a struct for file system configuration
type FileSystemConfig struct {
	ApplicationName string `yaml:"application_name,omitempty" json:"application_name,omitempty"`

	MetadataConnection ConnectionConfig `yaml:"metadata_connection,omitempty" json:"metadata_connection,omitempty"`
	IOConnection       ConnectionConfig `yaml:"io_connection,omitempty" json:"io_connection,omitempty"`

	Cache CacheConfig `yaml:"cache,omitempty" json:"cache,omitempty"`

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

	sessionConfig.ConnectionCreationTimeout = time.Duration(config.MetadataConnection.CreationTimeout)
	sessionConfig.ConnectionInitNumber = config.MetadataConnection.InitNumber
	sessionConfig.ConnectionLifespan = time.Duration(config.MetadataConnection.Lifespan)
	sessionConfig.OperationTimeout = time.Duration(config.MetadataConnection.OperationTimeout)
	sessionConfig.ConnectionIdleTimeout = time.Duration(config.MetadataConnection.IdleTimeout)
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

	sessionConfig.ConnectionCreationTimeout = time.Duration(config.IOConnection.CreationTimeout)
	sessionConfig.ConnectionInitNumber = config.IOConnection.InitNumber
	sessionConfig.ConnectionLifespan = time.Duration(config.IOConnection.Lifespan)
	sessionConfig.OperationTimeout = time.Duration(config.IOConnection.OperationTimeout)
	sessionConfig.ConnectionIdleTimeout = time.Duration(config.IOConnection.IdleTimeout)
	sessionConfig.ConnectionMaxNumber = config.IOConnection.MaxNumber
	sessionConfig.ConnectionMaxIdleNumber = config.IOConnection.MaxIdleNumber
	sessionConfig.TCPBufferSize = config.IOConnection.TCPBufferSize
	sessionConfig.StartNewTransaction = config.Cache.StartNewTransaction

	sessionConfig.AddressResolver = config.AddressResolver

	return sessionConfig
}
