package connection

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/auth"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

const (
	TCPBufferSizeDefault int = 4 * 1024 * 1024
)

// IRODSConnection connects to iRODS
type IRODSConnection struct {
	account         *types.IRODSAccount
	requestTimeout  time.Duration
	tcpBufferSize   int
	applicationName string

	connected               bool
	socket                  net.Conn
	serverVersion           *types.IRODSVersion
	generatedPasswordForPAM string // used for PAM auth
	creationTime            time.Time
	lastSuccessfulAccess    time.Time
	clientSignature         string
	dirtyTransaction        bool
	mutex                   sync.Mutex
	locked                  bool // true if mutex is locked

	metrics *metrics.IRODSMetrics
}

// NewIRODSConnection create a IRODSConnection
func NewIRODSConnection(account *types.IRODSAccount, requestTimeout time.Duration, applicationName string) *IRODSConnection {
	return &IRODSConnection{
		account:         account,
		requestTimeout:  requestTimeout,
		tcpBufferSize:   TCPBufferSizeDefault,
		applicationName: applicationName,

		creationTime:     time.Now(),
		clientSignature:  "",
		dirtyTransaction: false,
		mutex:            sync.Mutex{},

		metrics: &metrics.IRODSMetrics{},
	}
}

// NewIRODSConnectionWithMetrics create a IRODSConnection
func NewIRODSConnectionWithMetrics(account *types.IRODSAccount, requestTimeout time.Duration, applicationName string, metrics *metrics.IRODSMetrics) *IRODSConnection {
	return &IRODSConnection{
		account:         account,
		requestTimeout:  requestTimeout,
		tcpBufferSize:   TCPBufferSizeDefault,
		applicationName: applicationName,

		creationTime:     time.Now(),
		clientSignature:  "",
		dirtyTransaction: false,
		mutex:            sync.Mutex{},

		metrics: metrics,
	}
}

// Lock locks connection
func (conn *IRODSConnection) Lock() {
	conn.mutex.Lock()
	conn.locked = true
}

// Unlock unlocks connection
func (conn *IRODSConnection) Unlock() {
	conn.locked = false
	conn.mutex.Unlock()
}

// GetAccount returns iRODSAccount
func (conn *IRODSConnection) GetAccount() *types.IRODSAccount {
	return conn.account
}

// GetVersion returns iRODS version
func (conn *IRODSConnection) GetVersion() *types.IRODSVersion {
	return conn.serverVersion
}

// SetTCPBufferSize sets TCP Buffer Size
func (conn *IRODSConnection) SetTCPBufferSize(bufferSize int) {
	conn.tcpBufferSize = bufferSize
}

// SupportParallelUpload checks if the server supports parallel upload
// available from 4.2.9
func (conn *IRODSConnection) SupportParallelUpload() bool {
	return conn.serverVersion.HasHigherVersionThan(4, 2, 9)
}

func (conn *IRODSConnection) requiresCSNegotiation() bool {
	return conn.account.ClientServerNegotiation
}

// GetGeneratedPasswordForPAMAuth returns generated Password For PAM Auth
func (conn *IRODSConnection) GetGeneratedPasswordForPAMAuth() string {
	return conn.generatedPasswordForPAM
}

// IsConnected returns if the connection is live
func (conn *IRODSConnection) IsConnected() bool {
	return conn.connected
}

// GetCreationTime returns creation time
func (conn *IRODSConnection) GetCreationTime() time.Time {
	return conn.creationTime
}

// GetLastSuccessfulAccess returns last successful access time
func (conn *IRODSConnection) GetLastSuccessfulAccess() time.Time {
	return conn.lastSuccessfulAccess
}

// GetClientSignature returns client signature to be used in password obfuscation
func (conn *IRODSConnection) GetClientSignature() string {
	return conn.clientSignature
}

// SetTransactionDirty sets if transaction is dirty
func (conn *IRODSConnection) SetTransactionDirty(dirtyTransaction bool) {
	conn.dirtyTransaction = dirtyTransaction
}

// IsTransactionDirty returns true if transaction is dirty
func (conn *IRODSConnection) IsTransactionDirty() bool {
	return conn.dirtyTransaction
}

// Connect connects to iRODS
func (conn *IRODSConnection) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Connect",
	})

	conn.connected = false

	conn.account.FixAuthConfiguration()

	err := conn.account.Validate()
	if err != nil {
		return xerrors.Errorf("invalid account (%s): %w", err.Error(), types.NewConnectionConfigError())
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	server := fmt.Sprintf("%s:%d", conn.account.Host, conn.account.Port)
	logger.Debugf("Connecting to %s", server)

	socket, err := net.Dial("tcp", server)
	if err != nil {
		connErr := xerrors.Errorf("failed to connect to specified host %s and port %d (%s): %w", conn.account.Host, conn.account.Port, err.Error(), types.NewConnectionError())
		logger.Errorf("%+v", connErr)

		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	if tcpSocket, ok := socket.(*net.TCPConn); ok {
		sockErr := tcpSocket.SetReadBuffer(conn.tcpBufferSize)
		if sockErr != nil {
			sockBuffErr := xerrors.Errorf("failed to set tcp read buffer size %d: %w", conn.tcpBufferSize, sockErr)
			logger.Errorf("%+v", sockBuffErr)
		}

		sockErr = tcpSocket.SetWriteBuffer(conn.tcpBufferSize)
		if sockErr != nil {
			sockBuffErr := xerrors.Errorf("failed to set tcp write buffer size %d: %w", conn.tcpBufferSize, sockErr)
			logger.Errorf("%+v", sockBuffErr)
		}
	}

	if conn.metrics != nil {
		conn.metrics.IncreaseConnectionsOpened(1)
	}

	conn.socket = socket
	var irodsVersion *types.IRODSVersion

	if conn.requiresCSNegotiation() {
		// client-server negotiation
		irodsVersion, err = conn.connectWithCSNegotiation()
	} else {
		// No client-server negotiation
		irodsVersion, err = conn.connectWithoutCSNegotiation()
	}

	if err != nil {
		connErr := xerrors.Errorf("failed to startup an iRODS connection to server %s and port %d (%s): %w", conn.account.Host, conn.account.Port, err.Error(), types.NewConnectionError())
		logger.Errorf("%+v", connErr)
		_ = conn.disconnectNow()
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	conn.serverVersion = irodsVersion

	switch conn.account.AuthenticationScheme {
	case types.AuthSchemeNative:
		err = conn.loginNative(conn.account.Password)
	case types.AuthSchemeGSI:
		err = conn.loginGSI()
	case types.AuthSchemePAM:
		err = conn.loginPAM()
	default:
		logger.Errorf("unknown Authentication Scheme - %s", conn.account.AuthenticationScheme)
		return xerrors.Errorf("unknown Authentication Scheme - %s: %w", conn.account.AuthenticationScheme, types.NewConnectionConfigError())
	}

	if err != nil {
		connErr := xerrors.Errorf("failed to login to irods: %w", err)
		logger.Errorf("%+v", connErr)
		_ = conn.disconnectNow()
		return connErr
	}

	if conn.account.UseTicket() {
		req := message.NewIRODSMessageTicketAdminRequest("session", conn.account.Ticket)
		err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
		if err != nil {
			return xerrors.Errorf("received supply ticket error (%s): %w", err.Error(), types.NewAuthError())
		}
	}

	conn.connected = true
	conn.lastSuccessfulAccess = time.Now()

	return nil
}

func (conn *IRODSConnection) connectWithCSNegotiation() (*types.IRODSVersion, error) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "connectWithCSNegotiation",
	})

	// Get client negotiation policy
	clientPolicy := types.CSNegotiationRequireTCP
	if len(conn.account.CSNegotiationPolicy) > 0 {
		clientPolicy = conn.account.CSNegotiationPolicy
	}

	// Send a startup message
	logger.Debug("Start up a connection with CS Negotiation")

	startup := message.NewIRODSMessageStartupPack(conn.account, conn.applicationName, true)
	err := conn.RequestWithoutResponse(startup)
	if err != nil {
		return nil, xerrors.Errorf("failed to send startup (%s): %w", err.Error(), types.NewConnectionError())
	}

	// Server responds with negotiation response
	negotiationMessage, err := conn.ReadMessage(nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to receive negotiation message (%s): %w", err.Error(), types.NewConnectionError())
	}

	if negotiationMessage.Body == nil {
		return nil, xerrors.Errorf("failed to receive negotiation message body: %w", types.NewConnectionError())
	}

	if negotiationMessage.Body.Type == message.RODS_MESSAGE_VERSION_TYPE {
		// this happens when an error occur
		// Server responds with version
		version := message.IRODSMessageVersion{}
		err = version.FromMessage(negotiationMessage)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive negotiation message (%s): %w", err.Error(), types.NewConnectionError())
		}

		return version.GetVersion(), nil
	} else if negotiationMessage.Body.Type == message.RODS_MESSAGE_CS_NEG_TYPE {
		// Server responds with its own negotiation policy
		logger.Debug("Start up CS Negotiation")

		negotiation := message.IRODSMessageCSNegotiation{}
		err = negotiation.FromMessage(negotiationMessage)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive negotiation message (%s): %w", err.Error(), types.NewConnectionError())
		}

		serverPolicy, err := types.GetCSNegotiationRequire(negotiation.Result)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse server policy (%s): %w", err.Error(), types.NewConnectionError())
		}

		logger.Debugf("Client policy - %s, server policy - %s", clientPolicy, serverPolicy)

		// Perform the negotiation
		policyResult := types.PerformCSNegotiation(clientPolicy, serverPolicy)

		// If negotiation failed we're done
		if policyResult == types.CSNegotiationFailure {
			return nil, xerrors.Errorf("client-server negotiation failed - %s, %s: %w", string(clientPolicy), string(serverPolicy), types.NewConnectionError())
		}

		// Send negotiation result to server
		negotiationResult := message.NewIRODSMessageCSNegotiation(policyResult)
		version := message.IRODSMessageVersion{}
		err = conn.Request(negotiationResult, &version, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive version message (%s): %w", err.Error(), types.NewConnectionError())
		}

		if policyResult == types.CSNegotiationUseSSL {
			err := conn.sslStartup()
			if err != nil {
				return nil, xerrors.Errorf("failed to start up SSL: %w", err)
			}
		}

		return version.GetVersion(), nil
	}

	return nil, xerrors.Errorf("unknown response message '%s': %w", negotiationMessage.Body.Type, types.NewConnectionError())
}

func (conn *IRODSConnection) connectWithoutCSNegotiation() (*types.IRODSVersion, error) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "connectWithoutCSNegotiation",
	})

	// No client-server negotiation
	// Send a startup message
	logger.Debug("Start up connection without CS Negotiation")

	startup := message.NewIRODSMessageStartupPack(conn.account, conn.applicationName, false)
	version := message.IRODSMessageVersion{}
	err := conn.Request(startup, &version, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to receive version message (%s): %w", err.Error(), types.NewConnectionError())
	}

	return version.GetVersion(), nil
}

func (conn *IRODSConnection) sslStartup() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "sslStartup",
	})

	logger.Debug("Start up SSL")

	irodsSSLConfig := conn.account.SSLConfiguration
	if irodsSSLConfig == nil {
		return xerrors.Errorf("SSL Configuration is not set: %w", types.NewConnectionConfigError())
	}

	caCertPool := x509.NewCertPool()
	caCert, err := irodsSSLConfig.ReadCACert()
	if err != nil {
		logger.WithError(err).Warn("failed to read CA cert, ignoring...")
	} else {
		caCertPool.AppendCertsFromPEM(caCert)
	}

	sslConf := &tls.Config{
		RootCAs:            caCertPool,
		ServerName:         conn.account.Host,
		InsecureSkipVerify: true,
	}

	// Create a side connection using the existing socket
	sslSocket := tls.Client(conn.socket, sslConf)

	err = sslSocket.Handshake()
	if err != nil {
		return xerrors.Errorf("SSL Handshake error (%s): %w", err.Error(), types.NewConnectionError())
	}

	// from now on use ssl socket
	conn.socket = sslSocket

	// Generate a key (shared secret)
	encryptionKey := make([]byte, irodsSSLConfig.EncryptionKeySize)
	_, err = rand.Read(encryptionKey)
	if err != nil {
		return xerrors.Errorf("failed to generate shared secret (%s): %w", err.Error(), types.NewConnectionError())
	}

	// Send a ssl setting
	sslSetting := message.NewIRODSMessageSSLSettings(irodsSSLConfig.EncryptionAlgorithm, irodsSSLConfig.EncryptionKeySize, irodsSSLConfig.SaltSize, irodsSSLConfig.HashRounds)
	err = conn.RequestWithoutResponse(sslSetting)
	if err != nil {
		return xerrors.Errorf("failed to send ssl setting message (%s): %w", err.Error(), types.NewConnectionError())
	}

	// Send a shared secret
	sslSharedSecret := message.NewIRODSMessageSSLSharedSecret(encryptionKey)
	err = conn.RequestWithoutResponseNoXML(sslSharedSecret)
	if err != nil {
		return xerrors.Errorf("failed to send ssl shared secret message (%s): %w", err.Error(), types.NewConnectionError())
	}

	return nil
}

func (conn *IRODSConnection) login(password string) error {
	// authenticate
	authRequest := message.NewIRODSMessageAuthRequest()
	authChallenge := message.IRODSMessageAuthChallengeResponse{}
	err := conn.Request(authRequest, &authChallenge, nil)
	if err != nil {
		return xerrors.Errorf("failed to receive authentication challenge message body (%s): %w", err.Error(), types.NewAuthError())
	}

	challengeBytes, err := authChallenge.GetChallenge()
	if err != nil {
		return xerrors.Errorf("failed to get authentication challenge (%s): %w", err.Error(), types.NewAuthError())
	}

	// save client signature
	conn.clientSignature = conn.createClientSignature(challengeBytes)

	encodedPassword := auth.GenerateAuthResponse(challengeBytes, password)

	authResponse := message.NewIRODSMessageAuthResponse(encodedPassword, conn.account.ProxyUser)
	authResult := message.IRODSMessageAuthResult{}
	err = conn.RequestAndCheck(authResponse, &authResult, nil)
	if err != nil {
		return xerrors.Errorf("received irods authentication error (%s): %w", err.Error(), types.NewAuthError())
	}
	return nil
}

func (conn *IRODSConnection) loginNative(password string) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "loginNative",
	})

	logger.Debug("Logging in using native authentication method")
	return conn.login(password)
}

func (conn *IRODSConnection) loginGSI() error {
	return xerrors.Errorf("GSI login is not yet implemented: %w", types.NewAuthError())
}

func (conn *IRODSConnection) loginPAM() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "loginPAM",
	})

	logger.Debug("Logging in using pam authentication method")

	// Check whether ssl has already started, if not, start ssl.
	if _, ok := conn.socket.(*tls.Conn); !ok {
		return xerrors.Errorf("connection should be using SSL: %w", types.NewConnectionError())
	}

	ttl := conn.account.PamTTL
	if ttl <= 0 {
		ttl = 1
	}

	// authenticate
	pamAuthRequest := message.NewIRODSMessagePamAuthRequest(conn.account.ClientUser, conn.account.Password, ttl)
	pamAuthResponse := message.IRODSMessagePamAuthResponse{}
	err := conn.Request(pamAuthRequest, &pamAuthResponse, nil)
	if err != nil {
		return xerrors.Errorf("failed to receive an authentication challenge message (%s): %w", err.Error(), types.NewAuthError())
	}

	// save irods generated password for possible future use
	conn.generatedPasswordForPAM = pamAuthResponse.GeneratedPassword

	// retry native auth with generated password
	return conn.login(conn.generatedPasswordForPAM)
}

// Disconnect disconnects
func (conn *IRODSConnection) disconnectNow() error {
	conn.connected = false
	var err error
	if conn.socket != nil {
		err = conn.socket.Close()
		conn.socket = nil
	}

	if conn.metrics != nil {
		conn.metrics.DecreaseConnectionsOpened(1)
	}

	return xerrors.Errorf("failed to close socket: %w", err)
}

// Disconnect disconnects
func (conn *IRODSConnection) Disconnect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Disconnect",
	})

	logger.Debug("Disconnecting the connection")

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	disconnect := message.NewIRODSMessageDisconnect()
	err := conn.RequestWithoutResponse(disconnect)

	conn.lastSuccessfulAccess = time.Now()

	err2 := conn.disconnectNow()
	if err2 != nil {
		return err2
	}

	if err != nil {
		return err
	}

	return nil
}

func (conn *IRODSConnection) socketFail() {
	if conn.metrics != nil {
		conn.metrics.IncreaseCounterForConnectionFailures(1)
	}

	conn.disconnectNow()
}

// Send sends data
func (conn *IRODSConnection) Send(buffer []byte, size int) error {
	return conn.SendWithTrackerCallBack(buffer, size, nil)
}

// SendWithTrackerCallBack sends data
func (conn *IRODSConnection) SendWithTrackerCallBack(buffer []byte, size int, callback common.TrackerCallBack) error {
	if conn.socket == nil {
		return xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	// use sslSocket
	if conn.requestTimeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.requestTimeout))
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
func (conn *IRODSConnection) SendFromReader(src io.Reader, size int) error {
	if conn.socket == nil {
		return xerrors.Errorf("failed to send data - socket closed")
	}

	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	// use sslSocket
	if conn.requestTimeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.requestTimeout))
	}

	copyLen, err := io.CopyN(conn.socket, src, int64(size))
	if err != nil {
		conn.socketFail()
		return xerrors.Errorf("failed to send data: %w", err)
	}

	if copyLen != int64(size) {
		conn.socketFail()
		return xerrors.Errorf("failed to send data. failed to send data fully (requested %d vs sent %d)", size, copyLen)
	}

	if copyLen > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesSent(uint64(copyLen))
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// Recv receives a message
func (conn *IRODSConnection) Recv(buffer []byte, size int) (int, error) {
	return conn.RecvWithTrackerCallBack(buffer, size, nil)
}

// Recv receives a message
func (conn *IRODSConnection) RecvWithTrackerCallBack(buffer []byte, size int, callback common.TrackerCallBack) (int, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if conn.requestTimeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.requestTimeout))
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
func (conn *IRODSConnection) RecvToWriter(writer io.Writer, size int) (int, error) {
	if conn.socket == nil {
		return 0, xerrors.Errorf("failed to receive data - socket closed")
	}

	if !conn.locked {
		return 0, xerrors.Errorf("connection must be locked before use")
	}

	if conn.requestTimeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.requestTimeout))
	}

	copyLen, err := io.CopyN(writer, conn.socket, int64(size))
	if err != nil {
		conn.socketFail()
		return int(copyLen), xerrors.Errorf("failed to receive data: %w", err)
	}

	if copyLen > 0 {
		if conn.metrics != nil {
			conn.metrics.IncreaseBytesReceived(uint64(copyLen))
		}
	}

	conn.lastSuccessfulAccess = time.Now()

	return int(copyLen), nil
}

// SendMessage makes the message into bytes
func (conn *IRODSConnection) SendMessage(msg *message.IRODSMessage) error {
	return conn.SendMessageWithTrackerCallBack(msg, nil)
}

// SendMessageWithTrackerCallBack makes the message into bytes
func (conn *IRODSConnection) SendMessageWithTrackerCallBack(msg *message.IRODSMessage, callback common.TrackerCallBack) error {
	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	messageBuffer := new(bytes.Buffer)

	if msg.Header == nil && msg.Body == nil {
		return xerrors.Errorf("header and body cannot be nil")
	}

	var headerBytes []byte
	var err error

	messageLen := 0
	errorLen := 0
	bsLen := 0

	if msg.Body != nil {
		if msg.Body.Message != nil {
			messageLen = len(msg.Body.Message)
		}

		if msg.Body.Error != nil {
			errorLen = len(msg.Body.Error)
		}

		if msg.Body.Bs != nil {
			bsLen = len(msg.Body.Bs)
		}

		if msg.Header == nil {
			h := message.MakeIRODSMessageHeader(msg.Body.Type, uint32(messageLen), uint32(errorLen), uint32(bsLen), msg.Body.IntInfo)
			headerBytes, err = h.GetBytes()
			if err != nil {
				return err
			}
		}
	}

	if msg.Header != nil {
		headerBytes, err = msg.Header.GetBytes()
		if err != nil {
			return err
		}
	}

	// pack length - Big Endian
	headerLenBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(headerLenBuffer, uint32(len(headerBytes)))

	// header
	messageBuffer.Write(headerLenBuffer)
	messageBuffer.Write(headerBytes)

	if msg.Body != nil {
		bodyBytes, err := msg.Body.GetBytesWithoutBS()
		if err != nil {
			return err
		}

		// body
		messageBuffer.Write(bodyBytes)
	}

	// send
	bytes := messageBuffer.Bytes()
	err = conn.Send(bytes, len(bytes))
	if err != nil {
		return xerrors.Errorf("failed to send message: %w", err)
	}

	// send body-bs
	if msg.Body != nil {
		if msg.Body.Bs != nil {
			conn.SendWithTrackerCallBack(msg.Body.Bs, len(msg.Body.Bs), callback)
		}
	}
	return nil
}

// readMessageHeader reads data from the given connection and returns iRODSMessageHeader
func (conn *IRODSConnection) readMessageHeader() (*message.IRODSMessageHeader, error) {
	// read header size
	headerLenBuffer := make([]byte, 4)
	readLen, err := conn.Recv(headerLenBuffer, 4)
	if err != nil {
		return nil, xerrors.Errorf("failed to read header size: %w", err)
	}

	if readLen != 4 {
		return nil, xerrors.Errorf("failed to read header size, read %d", readLen)
	}

	headerSize := binary.BigEndian.Uint32(headerLenBuffer)
	if headerSize <= 0 {
		return nil, xerrors.Errorf("invalid header size returned - len = %d", headerSize)
	}

	// read header
	headerBuffer := make([]byte, headerSize)
	readLen, err = conn.Recv(headerBuffer, int(headerSize))
	if err != nil {
		return nil, xerrors.Errorf("failed to read header: %w", err)
	}
	if readLen != int(headerSize) {
		return nil, xerrors.Errorf("failed to read header fully - %d requested but %d read", headerSize, readLen)
	}

	header := message.IRODSMessageHeader{}
	err = header.FromBytes(headerBuffer)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

// ReadMessage reads data from the given socket and returns IRODSMessage
// if bsBuffer is given, bs data will be written directly to the bsBuffer
// if not given, a new buffer will be allocated.
func (conn *IRODSConnection) ReadMessage(bsBuffer []byte) (*message.IRODSMessage, error) {
	return conn.ReadMessageWithTrackerCallBack(bsBuffer, nil)
}

func (conn *IRODSConnection) ReadMessageWithTrackerCallBack(bsBuffer []byte, callback common.TrackerCallBack) (*message.IRODSMessage, error) {
	if !conn.locked {
		return nil, xerrors.Errorf("connection must be locked before use")
	}

	header, err := conn.readMessageHeader()
	if err != nil {
		return nil, err
	}

	// read body
	bodyLen := header.MessageLen + header.ErrorLen
	bodyBuffer := make([]byte, bodyLen)
	if bsBuffer == nil {
		bsBuffer = make([]byte, int(header.BsLen))
	} else if len(bsBuffer) < int(header.BsLen) {
		return nil, xerrors.Errorf("provided bs buffer is too short, %d size is given, but %d size is required", len(bsBuffer), int(header.BsLen))
	}

	bodyReadLen, err := conn.Recv(bodyBuffer, int(bodyLen))
	if err != nil {
		return nil, xerrors.Errorf("failed to read body: %w", err)
	}
	if bodyReadLen != int(bodyLen) {
		return nil, xerrors.Errorf("failed to read body fully - %d requested but %d read", bodyLen, bodyReadLen)
	}

	bsReadLen, err := conn.RecvWithTrackerCallBack(bsBuffer, int(header.BsLen), callback)
	if err != nil {
		return nil, xerrors.Errorf("failed to read body (BS): %w", err)
	}
	if bsReadLen != int(header.BsLen) {
		return nil, xerrors.Errorf("failed to read body (BS) fully - %d requested but %d read", int(header.BsLen), bsReadLen)
	}

	body := message.IRODSMessageBody{}
	err = body.FromBytes(header, bodyBuffer, bsBuffer[:int(header.BsLen)])
	if err != nil {
		return nil, err
	}

	body.Type = header.Type
	body.IntInfo = header.IntInfo

	return &message.IRODSMessage{
		Header: header,
		Body:   &body,
	}, nil
}

// Commit a transaction. This is useful in combination with the NO_COMMIT_FLAG.
// Usage is limited to privileged accounts.
func (conn *IRODSConnection) Commit() error {
	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	return conn.endTransaction(true)
}

// Rollback a transaction. This is useful in combination with the NO_COMMIT_FLAG.
// It can also be used to clear the current database transaction if there are no staged operations,
// just to refresh the view on the database for future queries.
// Usage is limited to privileged accounts.
func (conn *IRODSConnection) Rollback() error {
	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	return conn.endTransaction(false)
}

// PoorMansRollback rolls back a transaction as a nonprivileged account, bypassing API limitations.
// A nonprivileged account cannot have staged operations, so rollback is always a no-op.
// The usage for this function, is that rolling back the current database transaction still will start
// a new one, so that future queries will see all changes that where made up to calling this function.
func (conn *IRODSConnection) PoorMansRollback() error {
	if !conn.locked {
		return xerrors.Errorf("connection must be locked before use")
	}

	dummyCol := fmt.Sprintf("/%s/home/%s", conn.account.ClientZone, conn.account.ClientUser)

	return conn.poorMansEndTransaction(dummyCol, false)
}

func (conn *IRODSConnection) endTransaction(commit bool) error {
	request := message.NewIRODSMessageEndTransactionRequest(commit)
	response := message.IRODSMessageEndTransactionResponse{}
	return conn.RequestAndCheck(request, &response, nil)
}

func (conn *IRODSConnection) poorMansEndTransaction(dummyCol string, commit bool) error {
	request := message.NewIRODSMessageModifyCollectionRequest(dummyCol)
	if commit {
		request.AddKeyVal(common.COLLECTION_TYPE_KW, "NULL_SPECIAL_VALUE")
	}
	response := message.IRODSMessageModifyCollectionResponse{}
	err := conn.Request(request, &response, nil)
	if err != nil {
		return xerrors.Errorf("failed to make a poor mans end transaction")
	}

	if !commit {
		// We do expect an error on rollback because we didn't supply enough parameters
		if common.ErrorCode(response.Result) == common.CAT_INVALID_ARGUMENT {
			return nil
		}

		if response.Result == 0 {
			return xerrors.Errorf("expected an error, but transaction completed successfully")
		}
	}

	err = response.CheckError()
	if err != nil {
		return xerrors.Errorf("received irods error: %w", err)
	}
	return nil
}

// RawBind binds an IRODSConnection to a raw net.Conn socket - to be used for e.g. a proxy server setup
func (conn *IRODSConnection) RawBind(socket net.Conn) {
	conn.connected = true
	conn.socket = socket
}

// GetMetrics returns metrics
func (conn *IRODSConnection) GetMetrics() *metrics.IRODSMetrics {
	return conn.metrics
}

// createClientSignature creates a client signature from auth challenge
func (conn *IRODSConnection) createClientSignature(challenge []byte) string {
	if len(challenge) > 16 {
		challenge = challenge[:16]
	}

	signature := hex.EncodeToString(challenge)
	return signature
}
