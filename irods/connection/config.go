package connection

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

const (
	ApplicationNameDefault string        = "go-irodsclient"
	ConnectTimeoutDefault  time.Duration = 30 * time.Second // 30 seconds
	TcpBufferSizeDefault   int           = 0                // use system default

	OperationTimeoutDefault     time.Duration = 1 * time.Minute
	LongOperationTimeoutDefault time.Duration = 5 * time.Minute
)

type IRODSConnectionConfig struct {
	ConnectTimeout       time.Duration
	OperationTimeout     time.Duration
	LongOperationTimeout time.Duration
	ApplicationName      string
	// TcpBufferSize asks for this socket buffer size in both directions. Leave it at zero to
	// let the kernel size the buffers, which it does better than a fixed value in most cases.
	TcpBufferSize int
	// TcpSendBufferSize and TcpRecvBufferSize override TcpBufferSize for one direction.
	// A zero size falls back to TcpBufferSize.
	TcpSendBufferSize int
	TcpRecvBufferSize int

	Metrics  *metrics.IRODSMetrics // can be null
	Logger   *log.Logger           // can be nil
	LogEntry *log.Entry            // can be nil
}

type IRODSResourceServerConnectionConfig struct {
	ConnectTimeout time.Duration
	// TcpBufferSize asks for this socket buffer size in both directions. Leave it at zero to
	// let the kernel size the buffers.
	TcpBufferSize int
	// TcpSendBufferSize and TcpRecvBufferSize override TcpBufferSize for one direction.
	// A zero size falls back to TcpBufferSize.
	TcpSendBufferSize int
	TcpRecvBufferSize int

	Metrics *metrics.IRODSMetrics // can be null
}

func (connConfig *IRODSConnectionConfig) fillDefaults() {
	if connConfig.ConnectTimeout <= 0 {
		connConfig.ConnectTimeout = ConnectTimeoutDefault
	}

	if connConfig.OperationTimeout <= 0 {
		connConfig.OperationTimeout = OperationTimeoutDefault
	}

	if connConfig.LongOperationTimeout <= 0 {
		connConfig.LongOperationTimeout = LongOperationTimeoutDefault
	}

	if len(connConfig.ApplicationName) == 0 {
		connConfig.ApplicationName = ApplicationNameDefault
	}

	if connConfig.TcpBufferSize < 0 {
		connConfig.TcpBufferSize = 0
	}

	if connConfig.TcpSendBufferSize <= 0 {
		connConfig.TcpSendBufferSize = connConfig.TcpBufferSize
	}

	if connConfig.TcpRecvBufferSize <= 0 {
		connConfig.TcpRecvBufferSize = connConfig.TcpBufferSize
	}
}

func (connConfig *IRODSConnectionConfig) Validate() error {
	if len(connConfig.ApplicationName) == 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "application name is empty")
	}

	if connConfig.ConnectTimeout <= 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "connect timeout is invalid")
	}

	if connConfig.OperationTimeout <= 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "operation timeout is invalid")
	}

	if connConfig.LongOperationTimeout <= 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "long operation timeout is invalid")
	}

	if connConfig.TcpBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp buffer size is invalid")
	}

	return nil
}

func (connConfig *IRODSResourceServerConnectionConfig) fillDefaults() {
	if connConfig.ConnectTimeout <= 0 {
		connConfig.ConnectTimeout = ConnectTimeoutDefault
	}

	if connConfig.TcpBufferSize <= 0 {
		connConfig.TcpBufferSize = TcpBufferSizeDefault
	}

	if connConfig.TcpSendBufferSize <= 0 {
		connConfig.TcpSendBufferSize = connConfig.TcpBufferSize
	}

	if connConfig.TcpRecvBufferSize <= 0 {
		connConfig.TcpRecvBufferSize = connConfig.TcpBufferSize
	}
}

func (connConfig *IRODSResourceServerConnectionConfig) Validate() error {
	if connConfig.ConnectTimeout <= 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "connect timeout is invalid")
	}

	if connConfig.TcpBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp buffer size is invalid")
	}

	return nil
}
