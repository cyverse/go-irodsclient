package session

import (
	"time"
)

const (
	// IRODSSessionConnectionErrorTimeoutDefault is a default value of connection error timeout
	IRODSSessionConnectionErrorTimeoutDefault = 1 * time.Minute
	// IRODSSessionConnectionInitNumberDefault is a default value of connection init number
	IRODSSessionConnectionInitNumberDefault = 0
	// IRODSSessionConnectionMaxMin is a minimum value of connection max
	IRODSSessionConnectionMaxMin = 5
	// IRODSSessionConnectionMaxDefault is a default value of connection max
	IRODSSessionConnectionMaxDefault = 10
	// IRODSSessionConnectionLifespanDefault is a default value of connection lifespan
	IRODSSessionConnectionLifespanDefault = 1 * time.Hour
	// IRODSSessionTimeoutDefault is a default value of timeout
	IRODSSessionTimeoutDefault = 5 * time.Minute
	// IRODSSessionTCPBufferSizeDefault is a default value of tcp buffer size
	IRODSSessionTCPBufferSizeDefault = 4 * 1024 * 1024
)

// IRODSSessionConfig is for session configuration
type IRODSSessionConfig struct {
	ApplicationName        string
	ConnectionErrorTimeout time.Duration
	ConnectionLifespan     time.Duration
	OperationTimeout       time.Duration
	ConnectionIdleTimeout  time.Duration
	ConnectionMax          int
	ConnectionInitNumber   int
	ConnectionMaxIdle      int
	TCPBufferSize          int
	StartNewTransaction    bool
	AddressResolver        AddressResolver
}

// NewIRODSSessionConfig create a IRODSSessionConfig with a default settings
func NewIRODSSessionConfig(applicationName string) *IRODSSessionConfig {
	return &IRODSSessionConfig{
		ApplicationName:        applicationName,
		ConnectionErrorTimeout: IRODSSessionConnectionErrorTimeoutDefault,
		ConnectionLifespan:     IRODSSessionConnectionLifespanDefault,
		OperationTimeout:       IRODSSessionTimeoutDefault,
		ConnectionIdleTimeout:  IRODSSessionTimeoutDefault,
		ConnectionMax:          IRODSSessionConnectionMaxDefault,
		ConnectionInitNumber:   IRODSSessionConnectionInitNumberDefault,
		ConnectionMaxIdle:      IRODSSessionConnectionMaxMin,
		TCPBufferSize:          IRODSSessionTCPBufferSizeDefault,
		StartNewTransaction:    true,
		AddressResolver:        nil,
	}
}
