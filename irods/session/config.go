package session

import (
	"time"
)

const (
	// IRODSSessionConnectionCreationTimeoutDefault is a default value of connection error timeout
	IRODSSessionConnectionCreationTimeoutDefault = 1 * time.Minute
	// IRODSSessionConnectionInitNumberDefault is a default value of connection init
	IRODSSessionConnectionInitNumberDefault = 0
	// IRODSSessionConnectionMaxNumberDefault is a default value of connection max
	IRODSSessionConnectionMaxNumberDefault = 10
	// IRODSSessionConnectionLifespanDefault is a default value of connection lifespan
	IRODSSessionConnectionLifespanDefault = 1 * time.Hour
	// IRODSSessionOperationTimeoutDefault is a default value of operation timeout
	IRODSSessionOperationTimeoutDefault = 5 * time.Minute
	// IRODSSessionConnectionIdleTimeoutDefault is a default value of connection idle timeout
	IRODSSessionConnectionIdleTimeoutDefault = 5 * time.Minute
	// IRODSSessionConnectionMaxIdleNumberDefault is a default value of max idle connections
	IRODSSessionConnectionMaxIdleNumberDefault = 5
	// IRODSSessionTCPBufferSizeDefault is a default value of tcp buffer size
	IRODSSessionTCPBufferSizeDefault = 4 * 1024 * 1024
)

// IRODSSessionConfig is for session configuration
type IRODSSessionConfig struct {
	ApplicationName string

	ConnectionCreationTimeout time.Duration
	ConnectionInitNumber      int
	ConnectionMaxNumber       int
	ConnectionLifespan        time.Duration
	OperationTimeout          time.Duration
	ConnectionIdleTimeout     time.Duration
	ConnectionMaxIdleNumber   int
	TCPBufferSize             int

	StartNewTransaction bool

	AddressResolver AddressResolver
}

// NewIRODSSessionConfig create a IRODSSessionConfig with a default settings
func NewIRODSSessionConfig(applicationName string) *IRODSSessionConfig {
	return &IRODSSessionConfig{
		ApplicationName: applicationName,

		ConnectionCreationTimeout: IRODSSessionConnectionCreationTimeoutDefault,
		ConnectionInitNumber:      IRODSSessionConnectionInitNumberDefault,
		ConnectionMaxNumber:       IRODSSessionConnectionMaxNumberDefault,
		ConnectionLifespan:        IRODSSessionConnectionLifespanDefault,
		OperationTimeout:          IRODSSessionOperationTimeoutDefault,
		ConnectionIdleTimeout:     IRODSSessionConnectionIdleTimeoutDefault,
		ConnectionMaxIdleNumber:   IRODSSessionConnectionMaxIdleNumberDefault,
		TCPBufferSize:             IRODSSessionTCPBufferSizeDefault,
		StartNewTransaction:       true,

		AddressResolver: nil,
	}
}
