package fs

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
)

const (
	// FileSystemApplicationNameDefault is a default value of application name
	FileSystemApplicationNameDefault string = session.IRODSSessionApplicationNameDefault

	// Connection Config
	// FileSystemConnectionCreationTimeoutDefault is a default timeout value of connection error
	FileSystemConnectionCreationTimeoutDefault time.Duration = session.IRODSSessionConnectionCreationTimeoutDefault
	// FileSystemTcpBufferSizeDefault is a default value of tcp buffer size
	FileSystemTcpBufferSizeDefault int = session.IRODSSessionTcpBufferSizeDefault
	// FileSystemConnectionLifespanDefault is a default lifespan of a connection
	FileSystemConnectionLifespanDefault time.Duration = session.IRODSSessionConnectionLifespanDefault
	// FileSystemConnectionIdleTimeoutDefault is a default value of connection idle timeout
	FileSystemConnectionIdleTimeoutDefault time.Duration = session.IRODSSessionConnectionIdleTimeoutDefault
	// FileSystemOperationTimeout is a default value of operation timeout
	FileSystemOperationTimeout time.Duration = session.IRODSSessionOperationTimeoutDefault
	// FileSystemLongOperationTimeout is a default value of long operation timeout
	FileSystemLongOperationTimeout time.Duration = session.IRODSSessionLongOperationTimeoutDefault
	// FileSystemCacheTimeout is a default value of cache timeout
	FileSystemCacheTimeout time.Duration = 1 * time.Minute

	// Metadata Connection

	// FileSystemMetadataConnectionInitNumberDefault is a default value of connection init number
	FileSystemMetadataConnectionInitNumberDefault int = 1
	// FileSystemMetadataConnectionMaxNumberDefault is a default number of connection max value
	FileSystemMetadataConnectionMaxNumberDefault int = 2
	// FileSystemMetadataConnectionMaxIdleNumberDefault is a default number of max idle connections
	FileSystemMetadataConnectionMaxIdleNumberDefault int = 2

	// IO Connection

	// FileSystemIOConnectionInitNumberDefault is a default value of connection init number
	FileSystemIOConnectionInitNumberDefault int = 0
	// FileSystemIOConnectionMaxNumberDefault is a default number of connection max value
	FileSystemIOConnectionMaxNumberDefault int = 8
	// FileSystemIOConnectionMaxIdleNumberDefault is a default number of max idle connections
	FileSystemIOConnectionMaxIdleNumberDefault int = 5
)

// ConnectionConfig is a struct that stores configuration for connections
type ConnectionConfig struct {
	CreationTimeout      types.Duration `yaml:"creation_timeout,omitempty" json:"creation_timeout,omitempty"`             // timeout for creating a new connection
	InitNumber           int            `yaml:"init_number,omitempty" json:"init_number,omitempty"`                       // number of connections created when init
	MaxNumber            int            `yaml:"max_number,omitempty" json:"max_number,omitempty"`                         // max number of connections
	MaxIdleNumber        int            `yaml:"max_idle_number,omitempty" json:"max_idle_number,omitempty"`               // max number of idle connections
	Lifespan             types.Duration `yaml:"lifespan,omitempty" json:"lifespan,omitempty"`                             // connection's lifespan (max time to be reused)
	IdleTimeout          types.Duration `yaml:"idle_timeout,omitempty" json:"idle_timeout,omitempty"`                     // time out for being idle, after this point the connection will be disposed
	OperationTimeout     types.Duration `yaml:"operation_timeout,omitempty" json:"operation_timeout,omitempty"`           // timeout for iRODS operations
	LongOperationTimeout types.Duration `yaml:"long_operation_timeout,omitempty" json:"long_operation_timeout,omitempty"` // timeout for long iRODS operations
	TcpBufferSize        int            `yaml:"tcp_buffer_size,omitempty" json:"tcp_buffer_size,omitempty"`               // buffer size
	WaitConnection       bool           `yaml:"wait_connection,omitempty" json:"wait_connection,omitempty"`               // whether to wait for a connection to be available
}

// NewDefaultMetadataConnectionConfig creates a default ConnectionConfig for metadata
func NewDefaultMetadataConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:      types.Duration(FileSystemConnectionCreationTimeoutDefault),
		InitNumber:           FileSystemMetadataConnectionInitNumberDefault,
		MaxNumber:            FileSystemMetadataConnectionMaxNumberDefault,
		MaxIdleNumber:        FileSystemMetadataConnectionMaxIdleNumberDefault,
		Lifespan:             types.Duration(FileSystemConnectionLifespanDefault),
		IdleTimeout:          types.Duration(FileSystemConnectionIdleTimeoutDefault),
		OperationTimeout:     types.Duration(FileSystemOperationTimeout),
		LongOperationTimeout: types.Duration(FileSystemLongOperationTimeout),
		TcpBufferSize:        FileSystemTcpBufferSizeDefault,
		WaitConnection:       true,
	}
}

// NewDefaultIOConnectionConfig creates a default ConnectionConfig for IO
func NewDefaultIOConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		CreationTimeout:      types.Duration(FileSystemConnectionCreationTimeoutDefault),
		InitNumber:           FileSystemIOConnectionInitNumberDefault,
		MaxNumber:            FileSystemIOConnectionMaxNumberDefault,
		MaxIdleNumber:        FileSystemIOConnectionMaxIdleNumberDefault,
		Lifespan:             types.Duration(FileSystemConnectionLifespanDefault),
		IdleTimeout:          types.Duration(FileSystemConnectionIdleTimeoutDefault),
		OperationTimeout:     types.Duration(FileSystemOperationTimeout),
		LongOperationTimeout: types.Duration(FileSystemLongOperationTimeout),
		TcpBufferSize:        FileSystemTcpBufferSizeDefault,
		WaitConnection:       true,
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
	return &session.IRODSSessionConfig{
		ApplicationName: config.ApplicationName,

		ConnectionCreationTimeout: time.Duration(config.MetadataConnection.CreationTimeout),
		ConnectionInitNumber:      config.MetadataConnection.InitNumber,
		ConnectionMaxNumber:       config.MetadataConnection.MaxNumber,
		ConnectionLifespan:        time.Duration(config.MetadataConnection.Lifespan),
		ConnectionIdleTimeout:     time.Duration(config.MetadataConnection.IdleTimeout),
		ConnectionMaxIdleNumber:   config.MetadataConnection.MaxIdleNumber,
		OperationTimeout:          time.Duration(config.MetadataConnection.OperationTimeout),
		LongOperationTimeout:      time.Duration(config.MetadataConnection.LongOperationTimeout),
		TcpBufferSize:             config.MetadataConnection.TcpBufferSize,
		StartNewTransaction:       config.Cache.StartNewTransaction,
		WaitConnection:            config.MetadataConnection.WaitConnection,
		AddressResolver:           config.AddressResolver,
	}
}

// ToIOSessionConfig creates a IRODSSessionConfig from FileSystemConfig
func (config *FileSystemConfig) ToIOSessionConfig() *session.IRODSSessionConfig {
	return &session.IRODSSessionConfig{
		ApplicationName: config.ApplicationName,

		ConnectionCreationTimeout: time.Duration(config.IOConnection.CreationTimeout),
		ConnectionInitNumber:      config.IOConnection.InitNumber,
		ConnectionMaxNumber:       config.IOConnection.MaxNumber,
		ConnectionLifespan:        time.Duration(config.IOConnection.Lifespan),
		ConnectionIdleTimeout:     time.Duration(config.IOConnection.IdleTimeout),
		ConnectionMaxIdleNumber:   config.IOConnection.MaxIdleNumber,
		OperationTimeout:          time.Duration(config.IOConnection.OperationTimeout),
		LongOperationTimeout:      time.Duration(config.IOConnection.LongOperationTimeout),
		TcpBufferSize:             config.IOConnection.TcpBufferSize,
		StartNewTransaction:       config.Cache.StartNewTransaction,
		WaitConnection:            config.IOConnection.WaitConnection,
		AddressResolver:           config.AddressResolver,
	}
}
