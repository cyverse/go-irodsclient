package connection

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
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

// IRODSConnection connects to iRODS
type IRODSConnection struct {
	account *types.IRODSAccount
	config  *IRODSConnectionConfig

	connected            bool
	failed               bool
	isSSLSocket          bool
	socket               net.Conn
	serverVersion        *types.IRODSVersion
	sslSharedSecret      []byte
	creationTime         time.Time
	lastSuccessfulAccess time.Time
	clientSignature      string
	dirtyTransaction     bool
	mutex                sync.Mutex
	locked               bool // true if mutex is locked
}

// NewIRODSConnection create a IRODSConnection
func NewIRODSConnection(account *types.IRODSAccount, config *IRODSConnectionConfig) (*IRODSConnection, error) {
	if account == nil {
		return nil, xerrors.Errorf("account is not set: %w", types.NewConnectionConfigError(nil))
	}

	// use default config if not set
	if config == nil {
		config = &IRODSConnectionConfig{}
	}

	account.FixAuthConfiguration()
	err := account.Validate()
	if err != nil {
		return nil, err
	}

	config.fillDefaults()
	err = config.Validate()
	if err != nil {
		return nil, err
	}

	return &IRODSConnection{
		account: account,
		config:  config,

		creationTime:     time.Now(),
		clientSignature:  "",
		dirtyTransaction: false,
		mutex:            sync.Mutex{},
	}, nil
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

// SetWriteTimeout sets write timeout
func (conn *IRODSConnection) SetWriteTimeout(timeout time.Duration) {
	if conn.socket == nil {
		return
	}

	if !conn.locked {
		return
	}

	conn.socket.SetWriteDeadline(time.Now().Add(timeout))
}

// SetReadTimeout sets read timeout
func (conn *IRODSConnection) SetReadTimeout(timeout time.Duration) {
	if conn.socket == nil {
		return
	}

	if !conn.locked {
		return
	}

	conn.socket.SetReadDeadline(time.Now().Add(timeout))
}

// SupportParallelUpload checks if the server supports parallel upload
// available from 4.2.9
func (conn *IRODSConnection) SupportParallelUpload() bool {
	return conn.serverVersion.HasHigherVersionThan(4, 2, 9)
}

func (conn *IRODSConnection) requreNewPamAuth() bool {
	return conn.serverVersion.HasHigherVersionThan(4, 3, 0)
}

func (conn *IRODSConnection) requiresCSNegotiation() bool {
	return conn.account.ClientServerNegotiation
}

// GetPAMToken returns server generated token For PAM Auth
func (conn *IRODSConnection) GetPAMToken() string {
	return conn.account.PAMToken
}

// GetSSLSharedSecret returns ssl shared secret
func (conn *IRODSConnection) GetSSLSharedSecret() []byte {
	return conn.sslSharedSecret
}

// IsConnected returns if the connection is live
func (conn *IRODSConnection) IsConnected() bool {
	return conn.connected
}

func (conn *IRODSConnection) IsSocketFailed() bool {
	return conn.failed
}

// IsSSL returns if the connection is ssl
func (conn *IRODSConnection) IsSSL() bool {
	return conn.isSSLSocket
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

// setSocketOpt sets socket opts
func (conn *IRODSConnection) setSocketOpt(socket net.Conn, bufferSize int) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "setSocketOpt",
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

func (conn *IRODSConnection) connectTCP() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "connectTCP",
	})

	server := fmt.Sprintf("%s:%d", conn.account.Host, conn.account.Port)
	logger.Debugf("Connecting to %s", server)

	// must connect to the server within ConnectTimeout
	var dialer net.Dialer
	ctx, cancelFunc := context.WithTimeout(context.Background(), conn.config.ConnectTimeout)
	defer cancelFunc()

	socket, err := dialer.DialContext(ctx, "tcp", server)
	if err != nil {
		connErr := xerrors.Errorf("failed to connect to specified host %q and port %d (%s): %w", conn.account.Host, conn.account.Port, err.Error(), types.NewConnectionError())

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
	return nil
}

// Connect connects to iRODS
func (conn *IRODSConnection) Connect() error {
	conn.connected = false

	timeout := conn.GetOperationTimeout()

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// connect TCP
	err := conn.connectTCP()
	if err != nil {
		return err
	}

	irodsVersion, err := conn.startup()
	if err != nil {
		connErr := xerrors.Errorf("failed to startup an iRODS connection to server %q and port %d: %w", conn.account.Host, conn.account.Port, err)
		_ = conn.disconnectNow()
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
		}
		return connErr
	}

	conn.serverVersion = irodsVersion

	switch conn.account.AuthenticationScheme {
	case types.AuthSchemeNative:
		err = conn.loginNative()
	case types.AuthSchemeGSI:
		err = conn.loginGSI()
	case types.AuthSchemePAM, types.AuthSchemePAMPassword:
		if len(conn.account.PAMToken) > 0 {
			err = conn.loginPAMWithToken()
		} else {
			err = conn.loginPAMWithPassword()
			if err != nil {
				return xerrors.Errorf("failed to login to irods using PAM authentication: %w", err)
			}

			// reconnect when success
			_ = conn.logout()
			conn.disconnectNow()

			// connect TCP
			err = conn.connectTCP()
			if err != nil {
				return err
			}

			_, err = conn.startup()
			if err != nil {
				connErr := xerrors.Errorf("failed to startup an iRODS connection to server %q and port %d (%s): %w", conn.account.Host, conn.account.Port, err.Error(), types.NewConnectionError())
				_ = conn.logout()
				_ = conn.disconnectNow()
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
				}
				return connErr
			}

			err = conn.loginPAMWithToken()
		}
	default:
		err = xerrors.Errorf("unknown Authentication Scheme %q: %w", conn.account.AuthenticationScheme, types.NewConnectionConfigError(conn.account))
	}

	if err != nil {
		connErr := xerrors.Errorf("failed to login to irods: %w", err)
		_ = conn.logout()
		_ = conn.disconnectNow()
		return connErr
	}

	if conn.account.UseTicket() {
		req := message.NewIRODSMessageTicketAdminRequest("session", conn.account.Ticket)
		err := conn.RequestAndCheck(req, &message.IRODSMessageTicketAdminResponse{}, nil, timeout)
		if err != nil {
			_ = conn.logout()
			_ = conn.disconnectNow()
			return xerrors.Errorf("received supply ticket error (%s): %w", err.Error(), types.NewAuthError(conn.account))
		}
	}

	conn.connected = true
	conn.lastSuccessfulAccess = time.Now()
	return nil
}

func (conn *IRODSConnection) startup() (*types.IRODSVersion, error) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "startup",
	})

	clientPolicy := types.CSNegotiationPolicyRequestTCP
	if conn.requiresCSNegotiation() {
		// Get client negotiation policy
		if len(conn.account.CSNegotiationPolicy) > 0 {
			clientPolicy = conn.account.CSNegotiationPolicy
		}
	}

	logger.Debug("Start up an iRODS connection")

	timeout := conn.GetOperationTimeout()

	// Send a startup message
	startup := message.NewIRODSMessageStartupPack(conn.account, conn.config.ApplicationName, conn.requiresCSNegotiation())

	if conn.requiresCSNegotiation() {
		err := conn.RequestWithoutResponse(startup, timeout)
		if err != nil {
			return nil, xerrors.Errorf("failed to send startup (%s): %w", err.Error(), types.NewConnectionError())
		}
	} else {
		// no cs negotiation
		version := message.IRODSMessageVersion{}
		err := conn.Request(startup, &version, nil, timeout)
		if err != nil {
			// handle EOF
			if err == io.EOF {
				// this happens when server rejects the connection
				return nil, xerrors.Errorf("connection rejected (%s): %w", err.Error(), types.NewConnectionError())
			}

			return nil, xerrors.Errorf("failed to receive version message (%s): %w", err.Error(), types.NewConnectionError())
		}

		return version.GetVersion(), nil
	}

	// cs negotiation response
	negotiationMessage, err := conn.ReadMessage(nil, timeout.ResponseTimeout)
	if err != nil {
		if err == io.EOF {
			// this happens when server rejects the connection
			return nil, xerrors.Errorf("connection rejected (%s): %w", err.Error(), types.NewConnectionError())
		}

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

		serverPolicy := types.GetCSNegotiationPolicyRequest(negotiation.Result)

		logger.Debugf("Client policy %q, server policy %q", clientPolicy, serverPolicy)

		// Perform the negotiation
		policyResult := types.PerformCSNegotiation(clientPolicy, serverPolicy)

		// If negotiation failed we're done
		if policyResult == types.CSNegotiationFailure {
			return nil, xerrors.Errorf("client-server negotiation failed (client %q, server %q): %w", string(clientPolicy), string(serverPolicy), types.NewConnectionError())
		}

		// Send negotiation result to server
		negotiationResult := message.NewIRODSMessageCSNegotiation(policyResult)
		version := message.IRODSMessageVersion{}
		err = conn.Request(negotiationResult, &version, nil, timeout)
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

	return nil, xerrors.Errorf("unknown response message %q: %w", negotiationMessage.Body.Type, types.NewConnectionError())

}

func (conn *IRODSConnection) sslStartup() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "sslStartup",
	})

	logger.Debug("Start up SSL")

	timeout := conn.GetOperationTimeout()

	irodsSSLConfig := conn.account.SSLConfiguration
	if irodsSSLConfig == nil {
		return xerrors.Errorf("SSL Configuration is not set: %w", types.NewConnectionConfigError(conn.account))
	}

	tlsConfig, err := irodsSSLConfig.GetTLSConfig(conn.account.Host, true)
	if err != nil {
		return xerrors.Errorf("Failed to get TLS config: %w", err)
	}

	// Create a side connection using the existing socket
	sslSocket := tls.Client(conn.socket, tlsConfig)

	err = sslSocket.Handshake()
	if err != nil {
		return xerrors.Errorf("SSL Handshake error (%s): %w", err.Error(), types.NewConnectionError())
	}

	// from now on use ssl socket
	conn.socket = sslSocket
	conn.isSSLSocket = true

	// Generate a key (shared secret)
	encryptionKey := make([]byte, irodsSSLConfig.EncryptionKeySize)
	_, err = rand.Read(encryptionKey)
	if err != nil {
		return xerrors.Errorf("failed to generate shared secret (%s): %w", err.Error(), types.NewConnectionError())
	}

	// Send a ssl setting
	sslSetting := message.NewIRODSMessageSSLSettings(irodsSSLConfig.EncryptionAlgorithm, irodsSSLConfig.EncryptionKeySize, irodsSSLConfig.EncryptionSaltSize, irodsSSLConfig.EncryptionNumHashRounds)
	err = conn.RequestWithoutResponse(sslSetting, timeout)
	if err != nil {
		return xerrors.Errorf("failed to send ssl setting message (%s): %w", err.Error(), types.NewConnectionError())
	}

	// Send a shared secret
	sslSharedSecret := message.NewIRODSMessageSSLSharedSecret(encryptionKey)
	err = conn.RequestWithoutResponse(sslSharedSecret, timeout)
	if err != nil {
		return xerrors.Errorf("failed to send ssl shared secret message (%s): %w", err.Error(), types.NewConnectionError())
	}

	conn.sslSharedSecret = encryptionKey

	return nil
}

func (conn *IRODSConnection) login(password string) error {
	// authenticate
	timeout := conn.GetOperationTimeout()

	authRequest := message.NewIRODSMessageAuthRequest()
	authChallenge := message.IRODSMessageAuthChallengeResponse{}
	err := conn.RequestAndCheck(authRequest, &authChallenge, nil, timeout)
	if err != nil {
		return xerrors.Errorf("failed to receive authentication challenge message body (%s): %w", err.Error(), types.NewAuthError(conn.account))
	}

	challengeBytes, err := authChallenge.GetChallenge()
	if err != nil {
		return xerrors.Errorf("failed to get authentication challenge (%s): %w", err.Error(), types.NewAuthError(conn.account))
	}

	// save client signature
	conn.clientSignature = conn.createClientSignature(challengeBytes)

	encodedPassword := auth.GenerateAuthResponse(challengeBytes, password)

	authResponse := message.NewIRODSMessageAuthResponse(encodedPassword, conn.account.ProxyUser, conn.account.ProxyZone)
	authResult := message.IRODSMessageAuthResult{}
	err = conn.RequestAndCheck(authResponse, &authResult, nil, timeout)
	if err != nil {
		return xerrors.Errorf("received irods authentication error (%s): %w", err.Error(), types.NewAuthError(conn.account))
	}
	return nil
}

func (conn *IRODSConnection) loginNative() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "loginNative",
	})

	logger.Debug("Logging in using native authentication method")
	return conn.login(conn.account.Password)
}

func (conn *IRODSConnection) loginGSI() error {
	return xerrors.Errorf("GSI login is not yet implemented: %w", types.NewAuthError(conn.account))
}

func (conn *IRODSConnection) getSafePAMPassword(password string) string {
	// For certain characters in the pam password, if they need escaping with '\' then do so.
	replacer := strings.NewReplacer("@", "\\@", "=", "\\=", "&", "\\&", ";", "\\;")
	return replacer.Replace(password)
}

func (conn *IRODSConnection) loginPAMWithPassword() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "loginPAMWithPassword",
	})

	logger.Debug("Logging in using pam authentication method")

	timeout := conn.GetOperationTimeout()

	// Check whether ssl has already started, if not, start ssl.
	if _, ok := conn.socket.(*tls.Conn); !ok {
		return xerrors.Errorf("connection should be using SSL: %w", types.NewConnectionError())
	}

	ttl := conn.account.PamTTL
	if ttl < 0 {
		ttl = 0 // server decides
	}

	pamPassword := conn.getSafePAMPassword(conn.account.Password)

	userKV := fmt.Sprintf("a_user=%s", conn.account.ProxyUser)
	passwordKV := fmt.Sprintf("a_pw=%s", pamPassword)
	ttlKV := fmt.Sprintf("a_ttl=%s", strconv.Itoa(ttl))

	authContext := strings.Join([]string{userKV, passwordKV, ttlKV}, ";")

	useDedicatedPAMApi := true
	if conn.requreNewPamAuth() {
		useDedicatedPAMApi = strings.ContainsAny(pamPassword, ";=") || len(authContext) >= 1024+64
	}

	// authenticate
	pamToken := ""

	if useDedicatedPAMApi {
		logger.Debugf("use dedicated PAM api")

		pamAuthRequest := message.NewIRODSMessagePamAuthRequest(conn.account.ProxyUser, conn.account.Password, ttl)
		pamAuthResponse := message.IRODSMessagePamAuthResponse{}
		err := conn.RequestAndCheck(pamAuthRequest, &pamAuthResponse, nil, timeout)
		if err != nil {
			return xerrors.Errorf("failed to receive a PAM token (%s): %w", err.Error(), types.NewAuthError(conn.account))
		}

		pamToken = pamAuthResponse.GeneratedPassword
	} else {
		logger.Debugf("use auth plugin api: scheme %q", string(types.AuthSchemePAM))

		pamAuthRequest := message.NewIRODSMessageAuthPluginRequest(string(types.AuthSchemePAM), authContext)
		pamAuthResponse := message.IRODSMessageAuthPluginResponse{}
		err := conn.RequestAndCheck(pamAuthRequest, &pamAuthResponse, nil, timeout)
		if err != nil {
			return xerrors.Errorf("failed to receive a PAM token (%s): %w", err.Error(), types.NewAuthError(conn.account))
		}

		pamToken = string(pamAuthResponse.GeneratedPassword)
	}

	// save irods generated password for possible future use
	conn.account.PAMToken = pamToken

	// we do not login here.
	// connection will be disconnected and reconnected afterword
	return nil
}

func (conn *IRODSConnection) loginPAMWithToken() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "loginPAMWithToken",
	})

	logger.Debug("Logging in using pam authentication method")

	// Check whether ssl has already started, if not, start ssl.
	if _, ok := conn.socket.(*tls.Conn); !ok {
		return xerrors.Errorf("connection should be using SSL: %w", types.NewConnectionError())
	}

	// retry native auth with generated password
	return conn.login(conn.account.PAMToken)
}

// logout sends logout
func (conn *IRODSConnection) logout() error {
	timeout := conn.GetOperationTimeout()

	disconnect := message.NewIRODSMessageDisconnect()
	err := conn.RequestWithoutResponse(disconnect, timeout)

	conn.lastSuccessfulAccess = time.Now()

	if err != nil {
		return err
	}

	return nil
}

// Disconnect disconnects
func (conn *IRODSConnection) disconnectNow() error {
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

	err := conn.logout()

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
	if conn.config.Metrics != nil {
		conn.config.Metrics.IncreaseCounterForConnectionFailures(1)
	}

	conn.failed = true

	conn.disconnectNow()
}

// Send sends data
func (conn *IRODSConnection) Send(buffer []byte, size int, timeout *time.Duration) error {
	return conn.SendWithTrackerCallBack(buffer, size, timeout, nil)
}

// SendWithTrackerCallBack sends data
func (conn *IRODSConnection) SendWithTrackerCallBack(buffer []byte, size int, timeout *time.Duration, callback common.TrackerCallBack) error {
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
func (conn *IRODSConnection) SendFromReader(src io.Reader, size int64, timeout *time.Duration) (int64, error) {
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
func (conn *IRODSConnection) Recv(buffer []byte, size int, timeout *time.Duration) (int, error) {
	return conn.RecvWithTrackerCallBack(buffer, size, timeout, nil)
}

// Recv receives a message
func (conn *IRODSConnection) RecvWithTrackerCallBack(buffer []byte, size int, timeout *time.Duration, callback common.TrackerCallBack) (int, error) {
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
func (conn *IRODSConnection) RecvToWriter(writer io.Writer, size int64, timeout *time.Duration) (int64, error) {
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

// SendMessage makes the message into bytes
func (conn *IRODSConnection) SendMessage(msg *message.IRODSMessage, timeout time.Duration) error {
	return conn.SendMessageWithTrackerCallBack(msg, timeout, nil)
}

// SendMessageWithTrackerCallBack makes the message into bytes
func (conn *IRODSConnection) SendMessageWithTrackerCallBack(msg *message.IRODSMessage, timeout time.Duration, callback common.TrackerCallBack) error {
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
	conn.SetWriteTimeout(timeout)

	bytes := messageBuffer.Bytes()
	err = conn.Send(bytes, len(bytes), nil)
	if err != nil {
		return xerrors.Errorf("failed to send message: %w", err)
	}

	// send body-bs
	if msg.Body != nil {
		if msg.Body.Bs != nil {
			err = conn.SendWithTrackerCallBack(msg.Body.Bs, len(msg.Body.Bs), nil, callback)
			if err != nil {
				return xerrors.Errorf("failed to send message: %w", err)
			}
		}
	}
	return nil
}

// readMessageHeader reads data from the given connection and returns iRODSMessageHeader
func (conn *IRODSConnection) readMessageHeader() (*message.IRODSMessageHeader, error) {
	// read header size
	headerLenBuffer := make([]byte, 4)
	readLen, err := conn.Recv(headerLenBuffer, 4, nil)
	if err != nil {
		if err == io.EOF {
			// EOF means the connection is closed
			return nil, err
		}
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
	readLen, err = conn.Recv(headerBuffer, int(headerSize), nil)
	if err != nil {
		if err == io.EOF {
			// EOF means the connection is closed
			return nil, err
		}
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
func (conn *IRODSConnection) ReadMessage(bsBuffer []byte, timeout time.Duration) (*message.IRODSMessage, error) {
	return conn.ReadMessageWithTrackerCallBack(bsBuffer, timeout, nil)
}

func (conn *IRODSConnection) ReadMessageWithTrackerCallBack(bsBuffer []byte, timeout time.Duration, callback common.TrackerCallBack) (*message.IRODSMessage, error) {
	if !conn.locked {
		return nil, xerrors.Errorf("connection must be locked before use")
	}

	conn.SetReadTimeout(timeout)

	header, err := conn.readMessageHeader()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, xerrors.Errorf("failed to read message header: %w", err)
	}

	// read body
	bodyLen := header.MessageLen + header.ErrorLen
	bodyBuffer := make([]byte, bodyLen)
	if bsBuffer == nil {
		bsBuffer = make([]byte, int(header.BsLen))
	} else if len(bsBuffer) < int(header.BsLen) {
		return nil, xerrors.Errorf("provided bs buffer is too short, %d size is given, but %d size is required", len(bsBuffer), int(header.BsLen))
	}

	bodyReadLen, err := conn.Recv(bodyBuffer, int(bodyLen), nil)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, xerrors.Errorf("failed to read body: %w", err)
	}
	if bodyReadLen != int(bodyLen) {
		return nil, xerrors.Errorf("failed to read body fully - %d requested but %d read", bodyLen, bodyReadLen)
	}

	bsReadLen, err := conn.RecvWithTrackerCallBack(bsBuffer, int(header.BsLen), nil, callback)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, xerrors.Errorf("failed to read body (BS), len %d: %w", int(header.BsLen), err)
	}
	if bsReadLen != int(header.BsLen) {
		return nil, xerrors.Errorf("failed to read body (BS) fully - %d requested but read %d", int(header.BsLen), bsReadLen)
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

	dummyCol := conn.account.GetHomeDirPath()

	return conn.poorMansEndTransaction(dummyCol, false)
}

func (conn *IRODSConnection) endTransaction(commit bool) error {
	timeout := conn.GetOperationTimeout()

	request := message.NewIRODSMessageEndTransactionRequest(commit)
	response := message.IRODSMessageEndTransactionResponse{}
	return conn.RequestAndCheck(request, &response, nil, timeout)
}

func (conn *IRODSConnection) poorMansEndTransaction(dummyCol string, commit bool) error {
	timeout := conn.GetOperationTimeout()

	request := message.NewIRODSMessageModifyCollectionRequest(dummyCol)
	if commit {
		request.AddKeyVal(common.COLLECTION_TYPE_KW, "NULL_SPECIAL_VALUE")
	}

	response := message.IRODSMessageModifyCollectionResponse{}
	err := conn.Request(request, &response, nil, timeout)
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
	return conn.config.Metrics
}

// createClientSignature creates a client signature from auth challenge
func (conn *IRODSConnection) createClientSignature(challenge []byte) string {
	if len(challenge) > 16 {
		challenge = challenge[:16]
	}

	signature := hex.EncodeToString(challenge)
	return signature
}
