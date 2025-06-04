package session

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

const (
	// IRODSSessionApplicationNameDefault is a default value of application name
	IRODSSessionApplicationNameDefault string = connection.ApplicationNameDefault
	// IRODSSessionConnectionCreationTimeoutDefault is a default value of connection error timeout
	IRODSSessionConnectionCreationTimeoutDefault time.Duration = connection.ConnectTimeoutDefault
	// IRODSSessionTcpBufferSizeDefault is a default value of tcp buffer size
	IRODSSessionTcpBufferSizeDefault int = connection.TcpBufferSizeDefault
	// IRODSSessionConnectionInitNumberDefault is a default value of connection init
	IRODSSessionConnectionInitNumberDefault int = 0
	// IRODSSessionConnectionMaxNumberDefault is a default value of connection max
	IRODSSessionConnectionMaxNumberDefault int = 10
	// IRODSSessionConnectionLifespanDefault is a default value of connection lifespan
	IRODSSessionConnectionLifespanDefault time.Duration = 1 * time.Hour
	// IRODSSessionConnectionIdleTimeoutDefault is a default value of connection idle timeout
	IRODSSessionConnectionIdleTimeoutDefault time.Duration = 5 * time.Minute
	// IRODSSessionOperationTimeoutDefault is a default value of operation timeout
	IRODSSessionOperationTimeoutDefault time.Duration = connection.OperationTimeoutDefault
	// IRODSSessionLongOperationTimeoutDefault is a default value of long operation timeout
	IRODSSessionLongOperationTimeoutDefault time.Duration = connection.LongOperationTimeoutDefault

	// IRODSSessionConnectionMaxIdleNumberDefault is a default value of max idle connections
	IRODSSessionConnectionMaxIdleNumberDefault int = 5
)

// ConnectionPoolConfig is for connection pool configuration
type ConnectionPoolConfig struct {
	ApplicationName      string
	InitialCap           int
	MaxIdle              int
	MaxCap               int           // output warning if total connections exceeds maxcap number
	Lifespan             time.Duration // if a connection exceeds its lifespan, the connection will die
	IdleTimeout          time.Duration // if there's no activity on a connection for the timeout time, the connection will die
	ConnectTimeout       time.Duration // if there's no response for the timeout time, the connection will fail
	OperationTimeout     time.Duration // timeout for iRODS operations
	LongOperationTimeout time.Duration // timeout for long iRODS operations
	TcpBufferSize        int

	Metrics *metrics.IRODSMetrics // can be null
}

// IRODSSessionConfig is for session configuration
type IRODSSessionConfig struct {
	ApplicationName string

	ConnectionCreationTimeout time.Duration
	ConnectionInitNumber      int
	ConnectionMaxNumber       int
	ConnectionLifespan        time.Duration
	ConnectionIdleTimeout     time.Duration
	ConnectionMaxIdleNumber   int
	OperationTimeout          time.Duration // timeout for iRODS operations
	LongOperationTimeout      time.Duration // timeout for long iRODS operations
	TcpBufferSize             int
	StartNewTransaction       bool

	WaitConnection  bool            // if true, wait for a connection to be available when the pool is exhausted
	AddressResolver AddressResolver // can be nil
}

func (poolConfig *ConnectionPoolConfig) fillDefaults() {
	if len(poolConfig.ApplicationName) == 0 {
		poolConfig.ApplicationName = IRODSSessionApplicationNameDefault
	}

	if poolConfig.InitialCap < 0 {
		poolConfig.InitialCap = IRODSSessionConnectionInitNumberDefault
	}

	if poolConfig.MaxIdle < 0 {
		poolConfig.MaxIdle = IRODSSessionConnectionMaxIdleNumberDefault
	}

	if poolConfig.MaxCap <= 0 {
		poolConfig.MaxCap = IRODSSessionConnectionMaxNumberDefault
	}

	if poolConfig.Lifespan <= 0 {
		poolConfig.Lifespan = IRODSSessionConnectionLifespanDefault
	}

	if poolConfig.IdleTimeout <= 0 {
		poolConfig.IdleTimeout = IRODSSessionConnectionIdleTimeoutDefault
	}

	if poolConfig.ConnectTimeout <= 0 {
		poolConfig.ConnectTimeout = IRODSSessionConnectionCreationTimeoutDefault
	}

	if poolConfig.OperationTimeout <= 0 {
		poolConfig.OperationTimeout = IRODSSessionOperationTimeoutDefault
	}

	if poolConfig.LongOperationTimeout <= 0 {
		poolConfig.LongOperationTimeout = IRODSSessionLongOperationTimeoutDefault
	}

	if poolConfig.TcpBufferSize < 0 {
		poolConfig.TcpBufferSize = IRODSSessionTcpBufferSizeDefault
	}
}

func (poolConfig *ConnectionPoolConfig) Validate() error {
	if len(poolConfig.ApplicationName) == 0 {
		return xerrors.Errorf("application name is empty: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.InitialCap < 0 {
		return xerrors.Errorf("initial cap is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.MaxIdle < 0 {
		return xerrors.Errorf("max idle is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.MaxCap <= 0 {
		return xerrors.Errorf("max cap is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.Lifespan <= 0 {
		return xerrors.Errorf("lifespan is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.IdleTimeout <= 0 {
		return xerrors.Errorf("idle timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.ConnectTimeout <= 0 {
		return xerrors.Errorf("connect timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.OperationTimeout <= 0 {
		return xerrors.Errorf("operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.LongOperationTimeout <= 0 {
		return xerrors.Errorf("long operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if poolConfig.TcpBufferSize < 0 {
		return xerrors.Errorf("tcp buffer size is invalid: %w", types.NewConnectionConfigError(nil))
	}

	return nil
}

func (poolConfig *ConnectionPoolConfig) ToConnectionConfig() *connection.IRODSConnectionConfig {
	return &connection.IRODSConnectionConfig{
		ApplicationName:      poolConfig.ApplicationName,
		ConnectTimeout:       poolConfig.ConnectTimeout,
		OperationTimeout:     poolConfig.OperationTimeout,
		LongOperationTimeout: poolConfig.LongOperationTimeout,
		TcpBufferSize:        poolConfig.TcpBufferSize,
		Metrics:              poolConfig.Metrics,
	}
}

func (sessionConfig *IRODSSessionConfig) fillDefaults() {
	if len(sessionConfig.ApplicationName) == 0 {
		sessionConfig.ApplicationName = IRODSSessionApplicationNameDefault
	}

	if sessionConfig.ConnectionCreationTimeout <= 0 {
		sessionConfig.ConnectionCreationTimeout = IRODSSessionConnectionCreationTimeoutDefault
	}

	if sessionConfig.ConnectionInitNumber < 0 {
		sessionConfig.ConnectionInitNumber = IRODSSessionConnectionInitNumberDefault
	}

	if sessionConfig.ConnectionMaxNumber <= 0 {
		sessionConfig.ConnectionMaxNumber = IRODSSessionConnectionMaxNumberDefault
	}

	if sessionConfig.ConnectionLifespan <= 0 {
		sessionConfig.ConnectionLifespan = IRODSSessionConnectionLifespanDefault
	}

	if sessionConfig.ConnectionIdleTimeout <= 0 {
		sessionConfig.ConnectionIdleTimeout = IRODSSessionConnectionIdleTimeoutDefault
	}

	if sessionConfig.ConnectionMaxIdleNumber <= 0 {
		sessionConfig.ConnectionMaxIdleNumber = IRODSSessionConnectionMaxIdleNumberDefault
	}

	if sessionConfig.OperationTimeout <= 0 {
		sessionConfig.OperationTimeout = IRODSSessionOperationTimeoutDefault
	}

	if sessionConfig.LongOperationTimeout <= 0 {
		sessionConfig.LongOperationTimeout = IRODSSessionLongOperationTimeoutDefault
	}

	if sessionConfig.TcpBufferSize < 0 {
		sessionConfig.TcpBufferSize = IRODSSessionTcpBufferSizeDefault
	}
}

func (sessionConfig *IRODSSessionConfig) Validate() error {
	if len(sessionConfig.ApplicationName) == 0 {
		return xerrors.Errorf("application name is empty: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionCreationTimeout <= 0 {
		return xerrors.Errorf("connection creation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionInitNumber < 0 {
		return xerrors.Errorf("connection init number is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionMaxNumber <= 0 {
		return xerrors.Errorf("connection max number is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionLifespan <= 0 {
		return xerrors.Errorf("connection lifespan is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionIdleTimeout <= 0 {
		return xerrors.Errorf("connection idle timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.ConnectionMaxIdleNumber <= 0 {
		return xerrors.Errorf("connection max idle number is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.OperationTimeout <= 0 {
		return xerrors.Errorf("operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.LongOperationTimeout <= 0 {
		return xerrors.Errorf("long operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if sessionConfig.TcpBufferSize < 0 {
		return xerrors.Errorf("tcp buffer size is invalid: %w", types.NewConnectionConfigError(nil))
	}

	return nil
}

func (sessionConfig *IRODSSessionConfig) ToConnectionPoolConfig() *ConnectionPoolConfig {
	return &ConnectionPoolConfig{
		ApplicationName:      sessionConfig.ApplicationName,
		InitialCap:           sessionConfig.ConnectionInitNumber,
		MaxIdle:              sessionConfig.ConnectionMaxIdleNumber,
		MaxCap:               sessionConfig.ConnectionMaxNumber,
		Lifespan:             sessionConfig.ConnectionLifespan,
		IdleTimeout:          sessionConfig.ConnectionIdleTimeout,
		ConnectTimeout:       sessionConfig.ConnectionCreationTimeout,
		OperationTimeout:     sessionConfig.OperationTimeout,
		LongOperationTimeout: sessionConfig.LongOperationTimeout,
		TcpBufferSize:        sessionConfig.TcpBufferSize,
	}
}
