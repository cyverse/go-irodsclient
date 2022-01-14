package connection

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/cyverse/go-irodsclient/irods/auth"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"

	log "github.com/sirupsen/logrus"
)

// IRODSConnection connects to iRODS
type IRODSConnection struct {
	account         *types.IRODSAccount
	requestTimeout  time.Duration
	applicationName string

	// internal
	connected               bool
	socket                  net.Conn
	serverVersion           *types.IRODSVersion
	generatedPasswordForPAM string // used for PAM auth
	creationTime            time.Time
	lastSuccessfulAccess    time.Time
	transferMetrics         types.TransferMetrics
}

// NewIRODSConnection create a IRODSConnection
func NewIRODSConnection(account *types.IRODSAccount, requestTimeout time.Duration, applicationName string) *IRODSConnection {
	return &IRODSConnection{
		account:         account,
		requestTimeout:  requestTimeout,
		applicationName: applicationName,

		creationTime: time.Now(),
	}
}

// GetAccount returns iRODSAccount
func (conn *IRODSConnection) GetAccount() *types.IRODSAccount {
	return conn.account
}

// GetVersion returns iRODS version
func (conn *IRODSConnection) GetVersion() *types.IRODSVersion {
	return conn.serverVersion
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

// Connect connects to iRODS
func (conn *IRODSConnection) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Connect",
	})

	conn.connected = false

	server := fmt.Sprintf("%s:%d", conn.account.Host, conn.account.Port)
	logger.Debugf("Connecting to %s", server)

	socket, err := net.Dial("tcp", server)
	if err != nil {
		return fmt.Errorf("could not connect to specified host and port (%s:%d) - %v", conn.account.Host, conn.account.Port, err)
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
		_ = conn.disconnectNow()
		return err
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
		return fmt.Errorf("unknown Authentication Scheme - %s", conn.account.AuthenticationScheme)
	}

	if err != nil {
		_ = conn.disconnectNow()
		return err
	}

	if conn.account.UseTicket() {
		conn.showTicket()
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
	startupMessage, err := startup.GetMessage()
	if err != nil {
		return nil, fmt.Errorf("could not make a startup message - %v", err)
	}

	err = conn.SendMessage(startupMessage)
	if err != nil {
		return nil, fmt.Errorf("could not send a startup message - %v", err)
	}

	// Server responds with negotiation response
	negotiationMessage, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("could not receive a negotiation message - %v", err)
	}

	if negotiationMessage.Body == nil {
		return nil, fmt.Errorf("could not receive a negotiation message body")
	}

	if negotiationMessage.Body.Type == message.RODS_MESSAGE_VERSION_TYPE {
		// this happens when an error occur
		// Server responds with version
		version := message.IRODSMessageVersion{}
		err = version.FromMessage(negotiationMessage)
		if err != nil {
			return nil, fmt.Errorf("could not receive a negotiation message - %v", err)
		}

		return version.GetVersion(), nil
	} else if negotiationMessage.Body.Type == message.RODS_MESSAGE_CS_NEG_TYPE {
		// Server responds with its own negotiation policy
		logger.Debug("Start up CS Negotiation")

		negotiation := message.IRODSMessageCSNegotiation{}
		err = negotiation.FromMessage(negotiationMessage)
		if err != nil {
			return nil, fmt.Errorf("could not receive a negotiation message - %v", err)
		}

		serverPolicy, err := types.GetCSNegotiationRequire(negotiation.Result)
		if err != nil {
			return nil, fmt.Errorf("unable to parse server policy - %v", err)
		}

		logger.Debugf("Client policy - %s, server policy - %s", clientPolicy, serverPolicy)

		// Perform the negotiation
		policyResult, status := types.PerformCSNegotiation(clientPolicy, serverPolicy)

		// If negotiation failed we're done
		if policyResult == types.CSNegotiationFailure {
			return nil, fmt.Errorf("client-server negotiation failed: %s, %s", string(clientPolicy), string(serverPolicy))
		}

		// Send negotiation result to server
		negotiationResult := message.NewIRODSMessageCSNegotiation(status, policyResult)
		version := message.IRODSMessageVersion{}
		err = conn.Request(negotiationResult, &version)
		if err != nil {
			return nil, fmt.Errorf("could not receive a version message - %v", err)
		}

		if policyResult == types.CSNegotiationUseSSL {
			err := conn.sslStartup()
			if err != nil {
				return nil, fmt.Errorf("could not start up SSL - %v", err)
			}
		}

		return version.GetVersion(), nil
	}

	return nil, fmt.Errorf("unknown response message - %s", negotiationMessage.Body.Type)
}

func (conn *IRODSConnection) connectWithoutCSNegotiation() (*types.IRODSVersion, error) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "connectWithoutCSNegotiation",
	})

	// No client-server negotiation
	// Send a startup message
	logger.Debug("Start up a connection without CS Negotiation")

	startup := message.NewIRODSMessageStartupPack(conn.account, conn.applicationName, false)
	version := message.IRODSMessageVersion{}
	err := conn.Request(startup, &version)
	if err != nil {
		return nil, fmt.Errorf("could not receive a version message - %v", err)
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
		return fmt.Errorf("SSL Configuration is not set")
	}

	caCertPool := x509.NewCertPool()
	caCert, err := irodsSSLConfig.ReadCACert()
	if err == nil {
		caCertPool.AppendCertsFromPEM(caCert)
	}

	sslConf := &tls.Config{
		RootCAs:    caCertPool,
		ServerName: conn.account.Host,
	}

	// Create a side connection using the existing socket
	sslSocket := tls.Client(conn.socket, sslConf)

	err = sslSocket.Handshake()
	if err != nil {
		return fmt.Errorf("SSL Handshake error - %v", err)
	}

	// from now on use ssl socket
	conn.socket = sslSocket

	// Generate a key (shared secret)
	encryptionKey := make([]byte, irodsSSLConfig.EncryptionKeySize)
	_, err = rand.Read(encryptionKey)
	if err != nil {
		return fmt.Errorf("could not generate a shared secret - %v", err)
	}

	// Send a ssl setting
	sslSetting := message.NewIRODSMessageSSLSettings(irodsSSLConfig.EncryptionAlgorithm, irodsSSLConfig.EncryptionKeySize, irodsSSLConfig.SaltSize, irodsSSLConfig.HashRounds)
	sslSettingMessage, err := sslSetting.GetMessage()
	if err != nil {
		return fmt.Errorf("could not make a ssl setting message - %v", err)
	}

	err = conn.SendMessage(sslSettingMessage)
	if err != nil {
		return fmt.Errorf("could not send a ssl setting message - %v", err)
	}

	// Send a shared secret
	sslSharedSecret := message.NewIRODSMessageSSLSharedSecret(encryptionKey)
	sslSharedSecretMessage, err := sslSharedSecret.GetMessage()
	if err != nil {
		return fmt.Errorf("could not make a ssl shared secret message - %v", err)
	}

	err = conn.SendMessage(sslSharedSecretMessage)
	if err != nil {
		return fmt.Errorf("could not send a ssl shared secret message - %v", err)
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

	// authenticate
	authRequest := message.NewIRODSMessageAuthRequest()
	authChallenge := message.IRODSMessageAuthChallenge{}
	err := conn.Request(authRequest, &authChallenge)
	if err != nil {
		return fmt.Errorf("could not receive an authentication challenge message body")
	}

	encodedPassword, err := auth.GenerateAuthResponse(authChallenge.Challenge, password)
	if err != nil {
		return fmt.Errorf("could not generate an authentication response - %v", err)
	}

	authResponse := message.NewIRODSMessageAuthResponse(encodedPassword, conn.account.ProxyUser)
	authResult := message.IRODSMessageAuthResult{}
	return conn.RequestAndCheck(authResponse, &authResult)
}

func (conn *IRODSConnection) loginGSI() error {
	return nil
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
		return fmt.Errorf("connection should be using SSL")
	}

	ttl := conn.account.PamTTL
	if ttl <= 0 {
		ttl = 1
	}

	// authenticate
	pamAuthRequest := message.NewIRODSMessagePamAuthRequest(conn.account.ClientUser, conn.account.Password, ttl)
	pamAuthResponse := message.IRODSMessagePamAuthResponse{}
	err := conn.Request(pamAuthRequest, &pamAuthResponse)
	if err != nil {
		return fmt.Errorf("could not receive an authentication challenge message")
	}

	// save irods generated password for possible future use
	conn.generatedPasswordForPAM = pamAuthResponse.GeneratedPassword

	// retry native auth with generated password
	return conn.loginNative(conn.generatedPasswordForPAM)
}

func (conn *IRODSConnection) showTicket() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "showTicket",
	})

	logger.Debug("Submitting a ticket to obtain access")

	if len(conn.account.Ticket) > 0 {
		// show the ticket
		ticketRequest := message.NewIRODSMessageTicketAdminRequest("session", conn.account.Ticket)
		ticketResult := message.IRODSMessageTicketAdminResponse{}
		return conn.RequestAndCheck(ticketRequest, &ticketResult)
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
	return err
}

// Disconnect disconnects
func (conn *IRODSConnection) Disconnect() error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Disconnect",
	})

	logger.Debug("Disconnecting the connection")

	disconnect := message.NewIRODSMessageDisconnect()
	disconnectMessage, err := disconnect.GetMessage()
	if err != nil {
		return fmt.Errorf("could not make a disconnect request message - %v", err)
	}

	err = conn.SendMessage(disconnectMessage)
	if err != nil {
		return fmt.Errorf("could not send a disconnect request message - %v", err)
	}

	conn.lastSuccessfulAccess = time.Now()

	return conn.disconnectNow()
}

func (conn *IRODSConnection) socketFail() {
	conn.connected = false
	conn.socket = nil
}

// Send sends data
func (conn *IRODSConnection) Send(buffer []byte, size int) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Send",
	})

	if conn.socket == nil {
		return fmt.Errorf("unable to send data - socket closed")
	}

	// use sslSocket
	if conn.requestTimeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.requestTimeout))
	}

	err := util.WriteBytes(conn.socket, buffer, size)
	if err != nil {
		logger.Error("unable to send data. connection to remote host may have closed.")

		conn.socketFail()
		return fmt.Errorf("unable to send data - %v", err)
	}

	if size > 0 {
		conn.IncreaseTransferMetricsBytesSent(uint64(size))
	}

	conn.lastSuccessfulAccess = time.Now()

	return nil
}

// Recv receives a message
func (conn *IRODSConnection) Recv(buffer []byte, size int) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "Recv",
	})

	if conn.socket == nil {
		return 0, fmt.Errorf("unable to receive data - socket closed")
	}

	if conn.requestTimeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.requestTimeout))
	}

	readLen, err := util.ReadBytes(conn.socket, buffer, size)
	if err != nil {
		logger.Error("unable to receive data. connection to remote host may have closed.")

		conn.socketFail()
		return readLen, fmt.Errorf("unable to receive data - %v", err)
	}

	if readLen > 0 {
		conn.IncreaseTransferMetricsBytesReceived(uint64(readLen))
	}

	conn.lastSuccessfulAccess = time.Now()

	return readLen, nil
}

// SendMessage makes the message into bytes
func (conn *IRODSConnection) SendMessage(msg *message.IRODSMessage) error {
	messageBuffer := new(bytes.Buffer)

	if msg.Header == nil && msg.Body == nil {
		return fmt.Errorf("header and body cannot be nil")
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
		bodyBytes, err := msg.Body.GetBytes()
		if err != nil {
			return err
		}

		// body
		messageBuffer.Write(bodyBytes)
	}

	// send
	bytes := messageBuffer.Bytes()
	conn.Send(bytes, len(bytes))
	return nil
}

// readMessageHeader reads data from the given connection and returns iRODSMessageHeader
func (conn *IRODSConnection) readMessageHeader() (*message.IRODSMessageHeader, error) {
	// read header size
	headerLenBuffer := make([]byte, 4)
	readLen, err := conn.Recv(headerLenBuffer, 4)
	if readLen != 4 {
		return nil, fmt.Errorf("could not read header size")
	}
	if err != nil {
		return nil, fmt.Errorf("could not read header size - %v", err)
	}

	headerSize := binary.BigEndian.Uint32(headerLenBuffer)
	if headerSize <= 0 {
		return nil, fmt.Errorf("invalid header size returned - len = %d", headerSize)
	}

	// read header
	headerBuffer := make([]byte, headerSize)
	readLen, err = conn.Recv(headerBuffer, int(headerSize))
	if err != nil {
		return nil, fmt.Errorf("could not read header - %v", err)
	}
	if readLen != int(headerSize) {
		return nil, fmt.Errorf("could not read header fully - %d requested but %d read", headerSize, readLen)
	}

	header := message.IRODSMessageHeader{}
	err = header.FromBytes(headerBuffer)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

// ReadMessage reads data from the given socket and returns IRODSMessage
func (conn *IRODSConnection) ReadMessage() (*message.IRODSMessage, error) {
	header, err := conn.readMessageHeader()
	if err != nil {
		return nil, err
	}

	// read body
	bodyLen := header.MessageLen + header.ErrorLen + header.BsLen
	bodyBuffer := make([]byte, bodyLen)

	readLen, err := conn.Recv(bodyBuffer, int(bodyLen))
	if err != nil {
		return nil, fmt.Errorf("could not read body - %v", err)
	}
	if readLen != int(bodyLen) {
		return nil, fmt.Errorf("could not read body fully - %d requested but %d read", bodyLen, readLen)
	}

	body := message.IRODSMessageBody{}
	err = body.FromBytes(header, bodyBuffer)
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
	return conn.endTransaction(true)
}

// Rollback a transaction. This is useful in combination with the NO_COMMIT_FLAG.
// It can also be used to clear the current database transaction if there are no staged operations,
// just to refresh the view on the database for future queries.
// Usage is limited to privileged accounts.
func (conn *IRODSConnection) Rollback() error {
	return conn.endTransaction(false)
}

// PoorMansRollback rolls back a transaction as a nonprivileged account, bypassing API limitations.
// A nonprivileged account cannot have staged operations, so rollback is always a no-op.
// The usage for this function, is that rolling back the current database transaction still will start
// a new one, so that future queries will see all changes that where made up to calling this function.
func (conn *IRODSConnection) PoorMansRollback() error {
	dummyCol := fmt.Sprintf("/%s/home/%s", conn.account.ClientZone, conn.account.ClientUser)

	return conn.poorMansEndTransaction(dummyCol, false)
}

func (conn *IRODSConnection) endTransaction(commit bool) error {
	request := message.NewIRODSMessageEndTransactionRequest(commit)
	requestMessage, err := request.GetMessage()
	if err != nil {
		return fmt.Errorf("could not make a end transaction request message - %v", err)
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		return fmt.Errorf("could not send a end transaction request message - %v", err)
	}

	// Server responds with results
	responseMessage, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("could not receive a end transaction response message - %v", err)
	}

	response := message.IRODSMessageEndTransactionResponse{}
	err = response.FromMessage(responseMessage)
	if err != nil {
		return fmt.Errorf("could not receive a end transaction response message - %v", err)
	}

	err = response.CheckError()
	return err
}

func (conn *IRODSConnection) poorMansEndTransaction(dummyCol string, commit bool) error {
	request := message.NewIRODSMessageModColRequest(dummyCol)

	if commit {
		request.AddKeyVal(common.COLLECTION_TYPE_KW, "NULL_SPECIAL_VALUE")
	}

	requestMessage, err := request.GetMessage()
	if err != nil {
		return fmt.Errorf("could not make a poor mans end transaction request message - %v", err)
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		return fmt.Errorf("could not send a poor mans end transaction request message - %v", err)
	}

	// Server responds with results
	responseMessage, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("could not receive a poor mans end transaction response message - %v", err)
	}

	response := message.IRODSMessageModColResponse{}
	err = response.FromMessage(responseMessage)
	if err != nil {
		return fmt.Errorf("could not receive a poor mans end transaction response message - %v", err)
	}

	if !commit {
		// We do expect an error on rollback because we didn't supply enough parameters
		if common.ErrorCode(response.Result) == common.CAT_INVALID_ARGUMENT {
			return nil
		}

		if response.Result == 0 {
			return fmt.Errorf("expected an error, but transaction completed successfully")
		}
	}

	err = response.CheckError()
	return err
}

// RawBind binds an IRODSConnection to a raw net.Conn socket - to be used for e.g. a proxy server setup
func (conn *IRODSConnection) RawBind(socket net.Conn) {
	conn.connected = true
	conn.socket = socket
}

/*
 * Metrics related functions
 */

// GetTransferMetrics returns transfer metrics
func (conn *IRODSConnection) GetTransferMetrics() types.TransferMetrics {
	// returns a copy of metrics
	return conn.transferMetrics
}

// ClearTransferMetrics clears transfer metrics
func (conn *IRODSConnection) ClearTransferMetrics() {
	conn.transferMetrics = types.TransferMetrics{}
}

// IncreaseTransferMetricsBytesSent increases bytes sent metrics
func (conn *IRODSConnection) IncreaseTransferMetricsBytesSent(n uint64) {
	conn.transferMetrics.BytesSent += n
}

// IncreaseTransferMetricsBytesReceived increases bytes received metrics
func (conn *IRODSConnection) IncreaseTransferMetricsBytesReceived(n uint64) {
	conn.transferMetrics.BytesReceived += n
}

// IncreaseDataObjectMetricsStat increases stat data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsStat(n uint64) {
	conn.transferMetrics.DataObjectIO.Stat += n
}

// IncreaseDataObjectMetricsCreate increases create data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsCreate(n uint64) {
	conn.transferMetrics.DataObjectIO.Create += n
}

// IncreaseDataObjectMetricsDelete increases delete data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsDelete(n uint64) {
	conn.transferMetrics.DataObjectIO.Delete += n
}

// IncreaseDataObjectMetricsWrite increases write data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsWrite(n uint64) {
	conn.transferMetrics.DataObjectIO.Write += n
}

// IncreaseDataObjectMetricsRead increases read data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsRead(n uint64) {
	conn.transferMetrics.DataObjectIO.Read += n
}

// IncreaseDataObjectMetricsRename increases rename data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsRename(n uint64) {
	conn.transferMetrics.DataObjectIO.Rename += n
}

// IncreaseDataObjectMetricsMeta increases meta data object metrics
func (conn *IRODSConnection) IncreaseDataObjectMetricsMeta(n uint64) {
	conn.transferMetrics.DataObjectIO.Meta += n
}

// IncreaseCollectionMetricsStat increases stat collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsStat(n uint64) {
	conn.transferMetrics.CollectionIO.Stat += n
}

// IncreaseCollectionMetricsList increases list collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsList(n uint64) {
	conn.transferMetrics.CollectionIO.List += n
}

// IncreaseCollectionMetricsCreate increases create collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsCreate(n uint64) {
	conn.transferMetrics.CollectionIO.Create += n
}

// IncreaseCollectionMetricsDelete increases delete collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsDelete(n uint64) {
	conn.transferMetrics.CollectionIO.Delete += n
}

// IncreaseCollectionMetricsRename increases rename collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsRename(n uint64) {
	conn.transferMetrics.CollectionIO.Rename += n
}

// IncreaseCollectionMetricsMeta increases meta collection metrics
func (conn *IRODSConnection) IncreaseCollectionMetricsMeta(n uint64) {
	conn.transferMetrics.CollectionIO.Meta += n
}
