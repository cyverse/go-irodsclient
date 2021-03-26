package session

import (
	"time"
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
	initCap := 1
	maxIdle := 1
	if connectionMax >= 15 {
		maxIdle = 10
	} else if connectionMax >= 5 {
		maxIdle = 4
	}

	return &IRODSSessionConfig{
		ApplicationName:      applicationName,
		OperationTimeout:     operationTimeout,
		IdleTimeout:          idleTimeout,
		ConnectionMax:        connectionMax,
		ConnectionInitNumber: initCap,
		ConnectionMaxIdle:    maxIdle,
		StartNewTransaction:  startNewTransaction,
	}
}

// NewIRODSSessionConfigWithDefault create a IRODSSessionConfig with a default settings
func NewIRODSSessionConfigWithDefault(applicationName string) *IRODSSessionConfig {
	return &IRODSSessionConfig{
		ApplicationName:      applicationName,
		OperationTimeout:     5 * time.Minute,
		IdleTimeout:          5 * time.Minute,
		ConnectionMax:        20,
		ConnectionInitNumber: 1,
		ConnectionMaxIdle:    10,
		StartNewTransaction:  true,
	}
}
