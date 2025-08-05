package connection

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

// IRODSResourceServerConnection connects to iRODS resource server
type IRODSResourceServerConnection struct {
	controlConnection *IRODSConnection
	serverInfo        *types.IRODSRedirectionInfo
	config            *IRODSResourceServerConnectionConfig

	connected            bool
	socket               net.Conn
	creationTime         time.Time
	lastSuccessfulAccess time.Time
	mutex                sync.Mutex
	locked               bool // true if mutex is locked
}

// NewIRODSResourceServerConnection create a IRODSResourceServerConnection
func NewIRODSResourceServerConnection(controlConnection *IRODSConnection, redirectionInfo *types.IRODSRedirectionInfo, config *IRODSResourceServerConnectionConfig) (*IRODSResourceServerConnection, error) {
	if controlConnection == nil {
		return nil, xerrors.Errorf("control connection is not set: %w", types.NewResourceServerConnectionConfigError(nil))
	}

	if redirectionInfo == nil {
		return nil, xerrors.Errorf("redirection info is not set: %w", types.NewResourceServerConnectionConfigError(nil))
	}

	if config == nil {
		config = &IRODSResourceServerConnectionConfig{}
	}

	err := redirectionInfo.Validate()
	if err != nil {
		return nil, err
	}

	config.fillDefaults()
	err = config.Validate()
	if err != nil {
		return nil, err
	}

	return &IRODSResourceServerConnection{
		controlConnection: controlConnection,
		serverInfo:        redirectionInfo,
		config:            config,

		creationTime: time.Now(),
		mutex:        sync.Mutex{},
	}, nil
}

// Lock locks connection
func (conn *IRODSResourceServerConnection) Lock() {
	conn.mutex.Lock()
	conn.locked = true
}

// Unlock unlocks connection
func (conn *IRODSResourceServerConnection) Unlock() {
	conn.locked = false
	conn.mutex.Unlock()
}

// GetAccount returns iRODSAccount
func (conn *IRODSResourceServerConnection) GetServerInfo() *types.IRODSRedirectionInfo {
	return conn.serverInfo
}

// SetWriteTimeout sets write timeout
func (conn *IRODSResourceServerConnection) SetWriteTimeout(timeout time.Duration) {
	if conn.socket == nil {
		return
	}

	if !conn.locked {
		return
	}

	conn.socket.SetWriteDeadline(time.Now().Add(timeout))
}

// SetReadTimeout sets read timeout
func (conn *IRODSResourceServerConnection) SetReadTimeout(timeout time.Duration) {
	if conn.socket == nil {
		return
	}

	if !conn.locked {
		return
	}

	conn.socket.SetReadDeadline(time.Now().Add(timeout))
}

// IsConnected returns if the connection is live
func (conn *IRODSResourceServerConnection) IsConnected() bool {
	return conn.connected
}

// GetCreationTime returns creation time
func (conn *IRODSResourceServerConnection) GetCreationTime() time.Time {
	return conn.creationTime
}

// GetLastSuccessfulAccess returns last successful access time
func (conn *IRODSResourceServerConnection) GetLastSuccessfulAccess() time.Time {
	return conn.lastSuccessfulAccess
}

// setSocketOpt sets socket opts
func (conn *IRODSResourceServerConnection) setSocketOpt(socket net.Conn, bufferSize int) {
	logger := log.WithFields(log.Fields{
		"buffer_size": bufferSize,
	})

	if tcpSocket, ok := socket.(*net.TCPConn); ok {
		// TCP socket
		tcpSocket.SetNoDelay(true)

		tcpSocket.SetKeepAlive(true)
		tcpSocket.SetKeepAlivePeriod(15 * time.Second) // 15 seconds
		tcpSocket.SetLinger(5)                         // 5 seconds

		// TCP buffer size
		if bufferSize > 0 {
			sockErr := tcpSocket.SetReadBuffer(bufferSize)
			if sockErr != nil {
				sockBuffErr := xerrors.Errorf("failed to set tcp read buffer size %d: %w", bufferSize, sockErr)
				logger.Errorf("%+v", sockBuffErr)
			}

			sockErr = tcpSocket.SetWriteBuffer(bufferSize)
			if sockErr != nil {
				sockBuffErr := xerrors.Errorf("failed to set tcp write buffer size %d: %w", bufferSize, sockErr)
				logger.Errorf("%+v", sockBuffErr)
			}
		}
	}
}

// Connect connects to iRODS
func (conn *IRODSResourceServerConnection) Connect() error {
	logger := log.WithFields(log.Fields{})

	conn.connected = false

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	server := fmt.Sprintf("%s:%d", conn.serverInfo.Host, conn.serverInfo.Port)
	logger.Debugf("Connecting to %s", server)

	// must connect to the server within ConnectTimeout
	var dialer net.Dialer
	ctx, cancelFunc := context.WithTimeout(context.Background(), conn.config.ConnectTimeout)
	defer cancelFunc()

	socket, err := dialer.DialContext(ctx, "tcp", server)
	if err != nil {
		connErr := xerrors.Errorf("failed to connect to specified host %q and port %d (%s): %w", conn.serverInfo.Host, conn.serverInfo.Port, err.Error(), types.NewConnectionError())

		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	conn.setSocketOpt(socket, conn.config.TcpBufferSize)

	if conn.config.Metrics != nil {
		conn.config.Metrics.IncreaseConnectionsOpened(1)
	}

	conn.socket = socket

	auth := message.NewIRODSMessageResourceServerAuth(conn.serverInfo)
	authBytes, err := auth.GetBytes()
	if err != nil {
		connErr := xerrors.Errorf("failed to make authentication request (%s): %w", err.Error(), types.NewConnectionError())
		_ = conn.disconnectNow()
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	timeout := conn.controlConnection.GetOperationTimeout()

	err = conn.Send(authBytes, len(authBytes), &timeout.RequestTimeout)
	if err != nil {
		authErr := xerrors.Errorf("failed to send authentication request to server %q and port %d: %w", conn.serverInfo.Host, conn.serverInfo.Port, err)
		_ = conn.disconnectNow()
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
		}
		return authErr
	}

	conn.connected = true
	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// Disconnect disconnects
func (conn *IRODSResourceServerConnection) disconnectNow() error {
	conn.connected = false
	var err error
	if conn.socket != nil {
		err = conn.socket.Close()
		conn.socket = nil
	}

	if conn.config.Metrics != nil {
		conn.config.Metrics.DecreaseConnectionsOpened(1)
	}

	if err == nil {
		return nil
	}

	return xerrors.Errorf("failed to close socket: %w", err)
}

// Disconnect disconnects
func (conn *IRODSResourceServerConnection) Disconnect() error {
	logger := log.WithFields(log.Fields{})

	logger.Debug("Disconnecting the connection")

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	err := conn.disconnectNow()
	if err != nil {
		logger.WithError(err).Debug("failed to disconnect the connection")
		return err
	}

	logger.Debug("Disconnected the connection")
	return nil
}

func (conn *IRODSResourceServerConnection) socketFail() {
	if conn.config.Metrics != nil {
		conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
	}

	conn.disconnectNow()
}

// Send sends data
func (conn *IRODSResourceServerConnection) Send(buffer []byte, size int, timeout *time.Duration) error {
	return conn.SendWithTrackerCallBack(buffer, size, timeout, nil)
}

// SendWithTrackerCallBack sends data
func (conn *IRODSResourceServerConnection) SendWithTrackerCallBack(buffer []byte, size int, timeout *time.Duration, callback common.TransferTrackerCallback) error {
	if conn.socket == nil {
		return xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	if timeout != nil {
		conn.SetWriteTimeout(*timeout)
	}

	err := util.WriteBytesWithTrackerCallBack(conn.socket, buffer, size, callback)
	if err != nil {
		conn.socketFail()
		return xerrors.Errorf("failed to send data: %w", err)
	}

	if size > 0 {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseBytesSent(uint64(size))
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// SendFromReader sends data from Reader
func (conn *IRODSResourceServerConnection) SendFromReader(src io.Reader, size int64, timeout *time.Duration) (int64, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if timeout != nil {
		conn.SetWriteTimeout(*timeout)
	}

	copyLen, err := io.CopyN(conn.socket, src, size)
	if copyLen > 0 {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseBytesSent(uint64(copyLen))
		}
	}

	if err != nil {
		if err == io.EOF {
			return copyLen, io.EOF
		}

		conn.socketFail()
		return copyLen, xerrors.Errorf("failed to send data (req: %d, sent: %d): %w", size, copyLen, err)
	}

	conn.lastSuccessfulAccess = time.Now()

	return copyLen, nil
}

// Recv receives a message
func (conn *IRODSResourceServerConnection) Recv(buffer []byte, size int, timeout *time.Duration) (int, error) {
	return conn.RecvWithTrackerCallBack(buffer, size, timeout, nil)
}

// RecvWithTrackerCallBack receives a message
func (conn *IRODSResourceServerConnection) RecvWithTrackerCallBack(buffer []byte, size int, timeout *time.Duration, callback common.TransferTrackerCallback) (int, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if timeout != nil {
		conn.SetReadTimeout(*timeout)
	}

	readLen, err := util.ReadBytesWithTrackerCallBack(conn.socket, buffer, size, callback)
	if readLen > 0 {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseBytesReceived(uint64(readLen))
		}
	}

	if err != nil {
		if err == io.EOF {
			conn.lastSuccessfulAccess = time.Now()
			conn.disconnectNow()
			return readLen, io.EOF
		}

		conn.socketFail()
		return readLen, xerrors.Errorf("failed to receive data: %w", err)
	}

	conn.lastSuccessfulAccess = time.Now()

	return readLen, nil
}

// RecvToWriter receives a message to Writer
func (conn *IRODSResourceServerConnection) RecvToWriter(writer io.Writer, size int64, timeout *time.Duration) (int64, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if timeout != nil {
		conn.SetReadTimeout(*timeout)
	}

	copyLen, err := io.CopyN(writer, conn.socket, size)
	if copyLen > 0 {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseBytesReceived(uint64(copyLen))
		}
	}

	if err != nil {
		if err == io.EOF {
			conn.lastSuccessfulAccess = time.Now()
			conn.disconnectNow()
			return copyLen, io.EOF
		}

		conn.socketFail()
		return copyLen, xerrors.Errorf("failed to receive data: %w", err)
	}

	conn.lastSuccessfulAccess = time.Now()

	return copyLen, nil
}

// Decrypt decrypts byte buf
func (conn *IRODSResourceServerConnection) Decrypt(iv []byte, source []byte, dest []byte) (int, error) {
	if !conn.controlConnection.isSSLSocket {
		return 0, xerrors.Errorf("the connection is not SSL encrypted")
	}

	sslConf := conn.controlConnection.account.SSLConfiguration
	encryptionAlg := types.GetEncryptionAlgorithm(sslConf.EncryptionAlgorithm)

	len, err := util.Decrypt(encryptionAlg, conn.controlConnection.sslSharedSecret, iv, source, dest)
	if err != nil {
		return 0, xerrors.Errorf("failed to decrypt data: %w", err)
	}

	return len, nil
}

// Decrypt decrypts byte buf
func (conn *IRODSResourceServerConnection) Encrypt(iv []byte, source []byte, dest []byte) (int, error) {
	if !conn.controlConnection.isSSLSocket {
		return 0, xerrors.Errorf("the connection is not SSL encrypted")
	}

	sslConf := conn.controlConnection.account.SSLConfiguration
	encryptionAlg := types.GetEncryptionAlgorithm(sslConf.EncryptionAlgorithm)

	len, err := util.Encrypt(encryptionAlg, conn.controlConnection.sslSharedSecret, iv, source, dest)
	if err != nil {
		return 0, xerrors.Errorf("failed to encrypt data: %w", err)
	}

	return len, nil
}

// GetMetrics returns metrics
func (conn *IRODSResourceServerConnection) GetMetrics() *metrics.IRODSMetrics {
	return conn.config.Metrics
}
