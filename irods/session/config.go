package session

import (
	"time"
)

const (
	IRODSSessionConnectionMaxMin     = 5
	IRODSSessionConnectionMaxDefault = 10
	IRODSSessionIdleConnectionMax    = 5
	IRODSSessionTimeoutDefault       = 5 * time.Minute
)

// IRODSSessionConfig ...
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
