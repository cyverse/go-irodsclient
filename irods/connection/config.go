package connection

import (
	"time"

	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

const (
	ApplicationNameDefault string        = "go-irodsclient"
	ConnectTimeoutDefault  time.Duration = 30 * time.Second // 30 seconds
	TcpBufferSizeDefault   int           = 4 * 1024 * 1024

	OperationTimeoutDefault     time.Duration = 1 * time.Minute
	LongOperationTimeoutDefault time.Duration = 5 * time.Minute
)

type IRODSConnectionConfig struct {
	ConnectTimeout       time.Duration
	OperationTimeout     time.Duration
	LongOperationTimeout time.Duration
	ApplicationName      string
	TcpBufferSize        int

	Metrics *metrics.IRODSMetrics // can be null
}

type IRODSResourceServerConnectionConfig struct {
	ConnectTimeout time.Duration
	TcpBufferSize  int

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
		connConfig.TcpBufferSize = TcpBufferSizeDefault
	}
}

func (connConfig *IRODSConnectionConfig) Validate() error {
	if len(connConfig.ApplicationName) == 0 {
		return xerrors.Errorf("application name is empty: %w", types.NewConnectionConfigError(nil))
	}

	if connConfig.ConnectTimeout <= 0 {
		return xerrors.Errorf("connect timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if connConfig.OperationTimeout <= 0 {
		return xerrors.Errorf("operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if connConfig.LongOperationTimeout <= 0 {
		return xerrors.Errorf("long operation timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if connConfig.TcpBufferSize < 0 {
		return xerrors.Errorf("tcp buffer size is invalid: %w", types.NewConnectionConfigError(nil))
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
}

func (connConfig *IRODSResourceServerConnectionConfig) Validate() error {
	if connConfig.ConnectTimeout <= 0 {
		return xerrors.Errorf("connect timeout is invalid: %w", types.NewConnectionConfigError(nil))
	}

	if connConfig.TcpBufferSize < 0 {
		return xerrors.Errorf("tcp buffer size is invalid: %w", types.NewConnectionConfigError(nil))
	}

	return nil
}
