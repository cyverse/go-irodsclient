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
	tcpBufferSize     int

	connected            bool
	socket               net.Conn
	creationTime         time.Time
	lastSuccessfulAccess time.Time
	mutex                sync.Mutex
	locked               bool // true if mutex is locked

	metrics *metrics.IRODSMetrics
}

// NewIRODSResourceServerConnection create a IRODSResourceServerConnection
func NewIRODSResourceServerConnection(rootConnection *IRODSConnection, redirectionInfo *types.IRODSRedirectionInfo) *IRODSResourceServerConnection {
	tcpBufferSize := redirectionInfo.WindowSize
	if redirectionInfo.WindowSize <= 0 {
		tcpBufferSize = rootConnection.tcpBufferSize
	}

	return &IRODSResourceServerConnection{
		controlConnection: rootConnection,
		serverInfo:        redirectionInfo,
		tcpBufferSize:     tcpBufferSize,

		creationTime: time.Now(),
		mutex:        sync.Mutex{},

		metrics: &metrics.IRODSMetrics{},
	}
}

// NewIRODSResourceServerConnectionWithMetrics create a IRODSResourceServerConnection
func NewIRODSResourceServerConnectionWithMetrics(controlConnection *IRODSConnection, redirectionInfo *types.IRODSRedirectionInfo, metrics *metrics.IRODSMetrics) *IRODSResourceServerConnection {
	tcpBufferSize := redirectionInfo.WindowSize
	if redirectionInfo.WindowSize <= 0 {
		tcpBufferSize = TCPBufferSizeDefault
	}

	return &IRODSResourceServerConnection{
		controlConnection: controlConnection,
		serverInfo:        redirectionInfo,
		tcpBufferSize:     tcpBufferSize,

		creationTime: time.Now(),
		mutex:        sync.Mutex{},

		metrics: metrics,
	}
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
		"package":  "connection",
		"struct":   "IRODSResourceServerConnection",
		"function": "setSocketOpt",
	})

	if tcpSocket, ok := socket.(*net.TCPConn); ok {
		// TCP socket

		// nodelay is default
		//tcpSocket.SetNoDelay(true)

		tcpSocket.SetKeepAlive(true)

		// TCP buffer size
		if bufferSize <= 0 {
			bufferSize = TCPBufferSizeDefault
		}

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

// Connect connects to iRODS
func (conn *IRODSResourceServerConnection) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSResourceServerConnection",
		"function": "Connect",
	})

	conn.connected = false

	err := conn.serverInfo.Validate()
	if err != nil {
		return xerrors.Errorf("invalid resource server info (%s): %w", err.Error(), types.NewResourceServerConnectionConfigError(conn.serverInfo))
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	server := fmt.Sprintf("%s:%d", conn.serverInfo.Host, conn.serverInfo.Port)
	logger.Debugf("Connecting to %s", server)

	// must connect to the server in 10 sec
	var dialer net.Dialer
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	socket, err := dialer.DialContext(ctx, "tcp", server)
	if err != nil {
		connErr := xerrors.Errorf("failed to connect to specified host %s and port %d (%s): %w", conn.serverInfo.Host, conn.serverInfo.Port, err.Error(), types.NewConnectionError())
		logger.Errorf("%+v", connErr)

		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	conn.setSocketOpt(socket, conn.tcpBufferSize)

	if conn.metrics != nil {
		conn.metrics.IncreaseConnectionsOpened(1)
	}

	conn.socket = socket

	auth := message.NewIRODSMessageResourceServerAuth(conn.serverInfo)
	authBytes, err := auth.GetBytes()
	if err != nil {
		connErr := xerrors.Errorf("failed to make authentication request (%s): %w", err.Error(), types.NewConnectionError())
		logger.Errorf("%+v", connErr)
		_ = conn.disconnectNow()
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	err = conn.Send(authBytes, len(authBytes))
	if err != nil {
		authErr := xerrors.Errorf("failed to send authentication request to server %s and port %d: %w", conn.serverInfo.Host, conn.serverInfo.Port, err)
		logger.Errorf("%+v", authErr)
		_ = conn.disconnectNow()
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForConnectionFailures(1)
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

	if conn.metrics != nil {
		conn.metrics.DecreaseConnectionsOpened(1)
	}

	if err == nil {
		return nil
	}

	return xerrors.Errorf("failed to close socket: %w", err)
}

// Disconnect disconnects
func (conn *IRODSResourceServerConnection) Disconnect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSResourceServerConnection",
		"function": "Disconnect",
	})

	logger.Debug("Disconnecting the connection")

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	err := conn.disconnectNow()
	if err != nil {
		return err
	}

	return nil
}

func (conn *IRODSResourceServerConnection) socketFail() {
	if conn.metrics != nil {
		conn.metrics.IncreaseCounterForConnectionFailures(1)
	}

	conn.disconnectNow()
}

// Send sends data
func (conn *IRODSResourceServerConnection) Send(buffer []byte, size int) error {
	return conn.SendWithTrackerCallBack(buffer, size, nil)
}

// SendWithTrackerCallBack sends data
func (conn *IRODSResourceServerConnection) SendWithTrackerCallBack(buffer []byte, size int, callback common.TrackerCallBack) error {
	if conn.socket == nil {
		return xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	if conn.controlConnection.requestTimeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.controlConnection.requestTimeout))
	}

	err := util.WriteBytesWithTrackerCallBack(conn.socket, buffer, size, callback)
	if err != nil {
		conn.socketFail()
		return xerrors.Errorf("failed to send data: %w", err)
	}

	if size > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesSent(uint64(size))
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// SendFromReader sends data from Reader
func (conn *IRODSResourceServerConnection) SendFromReader(src io.Reader, size int64) error {
	if conn.socket == nil {
		return xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	if conn.controlConnection.requestTimeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.controlConnection.requestTimeout))
	}

	copyLen, err := io.CopyN(conn.socket, src, size)
	if copyLen != size {
		return xerrors.Errorf("failed to send data. failed to send data fully (requested %d vs sent %d)", size, copyLen)
	}

	if copyLen > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesSent(uint64(copyLen))
		}
	}

	if err != nil {
		if err != io.EOF {
			conn.socketFail()
			return xerrors.Errorf("failed to send data: %w", err)
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// Recv receives a message
func (conn *IRODSResourceServerConnection) Recv(buffer []byte, size int) (int, error) {
	return conn.RecvWithTrackerCallBack(buffer, size, nil)
}

// RecvWithTrackerCallBack receives a message
func (conn *IRODSResourceServerConnection) RecvWithTrackerCallBack(buffer []byte, size int, callback common.TrackerCallBack) (int, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if conn.controlConnection.requestTimeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.controlConnection.requestTimeout))
	}

	readLen, err := util.ReadBytesWithTrackerCallBack(conn.socket, buffer, size, callback)
	if err != nil {
		conn.socketFail()
		return readLen, xerrors.Errorf("failed to receive data: %w", err)
	}

	if readLen > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesReceived(uint64(readLen))
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return readLen, nil
}

// RecvToWriter receives a message to Writer
func (conn *IRODSResourceServerConnection) RecvToWriter(writer io.Writer, size int64) (int64, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if conn.controlConnection.requestTimeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.controlConnection.requestTimeout))
	}

	copyLen, err := io.CopyN(writer, conn.socket, size)
	if copyLen > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesReceived(uint64(copyLen))
		}
	}

	if err != nil {
		if err != io.EOF {
			conn.socketFail()
			return copyLen, xerrors.Errorf("failed to receive data: %w", err)
		}
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
	return conn.metrics
}
