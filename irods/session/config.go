package session

import (
	"time"
)

const (
	// IRODSSessionConnectionMaxMin is a minimum value for connection max
	IRODSSessionConnectionMaxMin = 5
	// IRODSSessionConnectionMaxDefault is a default value for connection max
	IRODSSessionConnectionMaxDefault = 10
	// IRODSSessionConnectionLifespanDefault is a default value for connection lifespan
	IRODSSessionConnectionLifespanDefault = 1 * time.Hour
	// IRODSSessionTimeoutDefault is a default value for timeout
	IRODSSessionTimeoutDefault = 5 * time.Minute
	// TCPBufferSizeDefault is a default value for tcp buffer size
	TCPBufferSizeDefault = 4 * 1024 * 1024
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
func NewIRODSSessionConfig(applicationName string, connectionLifespan time.Duration, operationTimeout time.Duration, idleTimeout time.Duration, connectionMax int, tcpBufferSize int, startNewTransaction bool) *IRODSSessionConfig {
	if connectionMax < IRODSSessionConnectionMaxMin {
		connectionMax = IRODSSessionConnectionMaxMin
	}

	return &IRODSSessionConfig{
		ApplicationName:       applicationName,
		ConnectionLifespan:    connectionLifespan,
		OperationTimeout:      operationTimeout,
		ConnectionIdleTimeout: idleTimeout,
		ConnectionMax:         connectionMax,
		ConnectionInitNumber:  1,
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
		ConnectionInitNumber:  1,
		ConnectionMaxIdle:     IRODSSessionConnectionMaxMin,
		TcpBufferSize:         TCPBufferSizeDefault,
		StartNewTransaction:   true,
	}
}
