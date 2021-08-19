package session

import (
	"time"
)

const (
	// IRODSSessionConnectionMaxMin is a minimum value for connection max
	IRODSSessionConnectionMaxMin = 5
	// IRODSSessionConnectionMaxDefault is a default value for connection max
	IRODSSessionConnectionMaxDefault = 10
	// IRODSSessionIdleConnectionMax is a maximum number of idle connections
	IRODSSessionIdleConnectionMax = 5
	// IRODSSessionTimeoutDefault is a default value for timeout
	IRODSSessionTimeoutDefault = 5 * time.Minute
)

// IRODSSessionConfig is for session configuration
type IRODSSessionConfig struct {
	ApplicationName      string
	OperationTimeout     time.Duration
	IdleTimeout          time.Duration
	ConnectionMax        int
	ConnectionInitNumber int
	ConnectionMaxIdle    int
	StartNewTransaction  bool
}

// NewIRODSSessionConfig create a IRODSSessionConfig
func NewIRODSSessionConfig(applicationName string, operationTimeout time.Duration, idleTimeout time.Duration, connectionMax int, startNewTransaction bool) *IRODSSessionConfig {
	if connectionMax < IRODSSessionConnectionMaxMin {
		connectionMax = IRODSSessionConnectionMaxMin
	}

	return &IRODSSessionConfig{
		ApplicationName:      applicationName,
		OperationTimeout:     operationTimeout,
		IdleTimeout:          idleTimeout,
		ConnectionMax:        connectionMax,
		ConnectionInitNumber: 1,
		ConnectionMaxIdle:    IRODSSessionIdleConnectionMax,
		StartNewTransaction:  startNewTransaction,
	}
}

// NewIRODSSessionConfigWithDefault create a IRODSSessionConfig with a default settings
func NewIRODSSessionConfigWithDefault(applicationName string) *IRODSSessionConfig {
	return &IRODSSessionConfig{
		ApplicationName:      applicationName,
		OperationTimeout:     IRODSSessionTimeoutDefault,
		IdleTimeout:          IRODSSessionTimeoutDefault,
		ConnectionMax:        IRODSSessionConnectionMaxDefault,
		ConnectionInitNumber: 1,
		ConnectionMaxIdle:    IRODSSessionIdleConnectionMax,
		StartNewTransaction:  true,
	}
}
