package session

import (
	"time"
)

const (
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
	ApplicationName       string
	ConnectionLifespan    time.Duration
	OperationTimeout      time.Duration
	ConnectionIdleTimeout time.Duration
	ConnectionMax         int
	ConnectionInitNumber  int
	ConnectionMaxIdle     int
	TcpBufferSize         int
	StartNewTransaction   bool
}

// NewIRODSSessionConfig create a IRODSSessionConfig
func NewIRODSSessionConfig(applicationName string, connectionInitNumber int, connectionLifespan time.Duration, operationTimeout time.Duration, idleTimeout time.Duration, connectionMax int, tcpBufferSize int, startNewTransaction bool) *IRODSSessionConfig {
	if connectionMax < IRODSSessionConnectionMaxMin {
		connectionMax = IRODSSessionConnectionMaxMin
	}

	return &IRODSSessionConfig{
		ApplicationName:       applicationName,
		ConnectionLifespan:    connectionLifespan,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: idleTimeout,
		ConnectionMax:         connectionMax,
		ConnectionInitNumber:  connectionInitNumber,
		ConnectionMaxIdle:     IRODSSessionConnectionMaxMin,
		TcpBufferSize:         tcpBufferSize,
		StartNewTransaction:   startNewTransaction,
	}
}

// NewIRODSSessionConfigWithDefault create a IRODSSessionConfig with a default settings
func NewIRODSSessionConfigWithDefault(applicationName string) *IRODSSessionConfig {
	return &IRODSSessionConfig{
		ApplicationName:       applicationName,
		ConnectionLifespan:    IRODSSessionConnectionLifespanDefault,
		OperationTimeout:      IRODSSessionTimeoutDefault,
		ConnectionIdleTimeout: IRODSSessionTimeoutDefault,
		ConnectionMax:         IRODSSessionConnectionMaxDefault,
		ConnectionInitNumber:  IRODSSessionConnectionInitNumberDefault,
		ConnectionMaxIdle:     IRODSSessionConnectionMaxMin,
		TcpBufferSize:         IRODSSessionTCPBufferSizeDefault,
		StartNewTransaction:   true,
	}
}
