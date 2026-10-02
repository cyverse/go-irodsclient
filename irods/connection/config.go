package connection

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

const (
	ApplicationNameDefault   string        = "go-irodsclient"
	ConnectTimeoutDefault    time.Duration = 30 * time.Second // 30 seconds
	TcpSendBufferSizeDefault int           = 0                // use system default
	TcpRecvBufferSizeDefault int           = 0                // use system default

	OperationTimeoutDefault     time.Duration = 1 * time.Minute
	LongOperationTimeoutDefault time.Duration = 5 * time.Minute
)

type IRODSConnectionConfig struct {
	ConnectTimeout       time.Duration
	OperationTimeout     time.Duration
	LongOperationTimeout time.Duration
	ApplicationName      string
	// TcpSendBufferSize and TcpRecvBufferSize ask for these socket buffer sizes. Leave one at
	// zero to let the kernel size that direction, which it does better than a fixed value in
	// most cases.
	TcpSendBufferSize int
	TcpRecvBufferSize int

	Metrics  *metrics.IRODSMetrics // can be null
	Logger   *log.Logger           // can be nil
	LogEntry *log.Entry            // can be nil
}

type IRODSResourceServerConnectionConfig struct {
	ConnectTimeout time.Duration
	// TcpSendBufferSize and TcpRecvBufferSize ask for these socket buffer sizes. Leave one at
	// zero to let the kernel size that direction.
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

	if connConfig.TcpSendBufferSize < 0 {
		connConfig.TcpSendBufferSize = TcpSendBufferSizeDefault
	}

	if connConfig.TcpRecvBufferSize < 0 {
		connConfig.TcpRecvBufferSize = TcpRecvBufferSizeDefault
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

	if connConfig.TcpSendBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp send buffer size is invalid")
	}

	if connConfig.TcpRecvBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp receive buffer size is invalid")
	}

	return nil
}

func (connConfig *IRODSResourceServerConnectionConfig) fillDefaults() {
	if connConfig.ConnectTimeout <= 0 {
		connConfig.ConnectTimeout = ConnectTimeoutDefault
	}

	if connConfig.TcpSendBufferSize < 0 {
		connConfig.TcpSendBufferSize = TcpSendBufferSizeDefault
	}

	if connConfig.TcpRecvBufferSize < 0 {
		connConfig.TcpRecvBufferSize = TcpRecvBufferSizeDefault
	}
}

func (connConfig *IRODSResourceServerConnectionConfig) Validate() error {
	if connConfig.ConnectTimeout <= 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "connect timeout is invalid")
	}

	if connConfig.TcpSendBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp send buffer size is invalid")
	}

	if connConfig.TcpRecvBufferSize < 0 {
		newErr := types.NewConnectionConfigError(nil)
		return errors.Wrapf(newErr, "tcp receive buffer size is invalid")
	}

	return nil
}
