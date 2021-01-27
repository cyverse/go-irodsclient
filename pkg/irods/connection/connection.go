package connection

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/message"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// IRODSConnection connects to iRODS
type IRODSConnection struct {
	Account         *types.IRODSAccount
	Timeout         time.Duration
	ApplicationName string

	// internal
	disconnected  bool
	socket        net.Conn
	serverVersion *types.IRODSVersion
}

// NewIRODSConnection create a IRODSConnection
func NewIRODSConnection(account *types.IRODSAccount, timeout time.Duration, applicationName string) *IRODSConnection {
	return &IRODSConnection{
		Account:         account,
		Timeout:         timeout,
		ApplicationName: applicationName,
	}
}

// GetVersion returns iRODS version
func (conn *IRODSConnection) GetVersion() *types.IRODSVersion {
	return conn.serverVersion
}

func (conn *IRODSConnection) requiresCSNegotiation() bool {
	return conn.Account.ClientServerNegotiation
}

// Connect connects to iRODS
func (conn *IRODSConnection) Connect() error {
	conn.disconnected = true

	server := fmt.Sprintf("%s:%d", conn.Account.Host, conn.Account.Port)
	socket, err := net.Dial("tcp", server)
	if err != nil {
		return fmt.Errorf("Could not connect to specified host and port (%s:%d) - %s", conn.Account.Host, conn.Account.Port, err.Error())
	}

	conn.socket = socket
	var irodsVersion *types.IRODSVersion
	if conn.requiresCSNegotiation() {
		// client-server negotiation
		util.LogInfo("Connect with CS Negotiation")
		irodsVersion, err = conn.connectWithCSNegotiation()
	} else {
		// No client-server negotiation
		util.LogInfo("Connect without CS Negotiation")
		irodsVersion, err = conn.connectWithoutCSNegotiation()
	}

	if err != nil {
		_ = conn.Disconnect()
		return err
	}

	conn.serverVersion = irodsVersion

	switch conn.Account.AuthenticationScheme {
	case types.AuthSchemeNative:
		err = conn.loginNative()
	case types.AuthSchemeGSI:
		err = conn.loginGSI()
	case types.AuthSchemePAM:
		err = conn.loginPAM()
	default:
		return fmt.Errorf("Unknown Authentication Scheme - %s", conn.Account.AuthenticationScheme)
	}

	if err != nil {
		_ = conn.Disconnect()
		return err
	}

	return nil
}

func (conn *IRODSConnection) connectWithCSNegotiation() (*types.IRODSVersion, error) {
	// Get client negotiation policy
	clientPolicy := types.CSNegotiationRequireTCP
	if len(conn.Account.CSNegotiationPolicy) > 0 {
		clientPolicy = conn.Account.CSNegotiationPolicy
	}

	// Send a startup message
	util.LogInfo("Start up a new connection")
	startupMessage := message.NewIRODSMessageStartupPack(conn.Account, conn.ApplicationName, true)
	startupMessageBody, err := startupMessage.GetMessageBody()
	if err != nil {
		return nil, fmt.Errorf("Could not make a startup message - %s", err.Error())
	}

	err = conn.SendMessage(nil, startupMessageBody)
	if err != nil {
		return nil, fmt.Errorf("Could not send a startup message - %s", err.Error())
	}

	// Server responds with negotiation response
	_, negotiationMessageBody, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("Could not receive a negotiation message - %s", err.Error())
	}

	if negotiationMessageBody == nil {
		return nil, fmt.Errorf("Could not receive a negotiation message body")
	}

	if negotiationMessageBody.Type == message.RODS_MESSAGE_VERSION_TYPE {
		// this happens when an error occur
		// Server responds with version
		versionMessage := message.IRODSMessageVersion{}
		err = versionMessage.FromMessageBody(negotiationMessageBody)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a negotiation message body")
		}

		return versionMessage.GetVersion(), nil
	} else if negotiationMessageBody.Type == message.RODS_MESSAGE_CS_NEG_TYPE {
		// Server responds with its own negotiation policy
		util.LogInfo("Start up CS Negotiation")
		negotiationMessage := message.IRODSMessageCSNegotiation{}
		err = negotiationMessage.FromMessageBody(negotiationMessageBody)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a negotiation message body")
		}

		serverPolicy, err := types.GetCSNegotiationRequire(negotiationMessage.Result)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse server policy - %s", negotiationMessage.Result)
		}

		// Perform the negotiation
		negotiationResult, status := types.PerformCSNegotiation(clientPolicy, serverPolicy)

		// If negotiation failed we're done
		if negotiationResult == types.CSNegotiationFailure {
			return nil, fmt.Errorf("Client-Server negotiation failed: %s, %s", string(clientPolicy), string(serverPolicy))
		}

		// Send negotiation result to server
		negotiationResultMessage := message.NewIRODSMessageCSNegotiation(status, negotiationResult)
		negotiationResultMessageBody, err := negotiationResultMessage.GetMessageBody()
		if err != nil {
			return nil, fmt.Errorf("Could not make a negotiation result message - %s", err.Error())
		}

		err = conn.SendMessage(nil, negotiationResultMessageBody)
		if err != nil {
			return nil, fmt.Errorf("Could not send a negotiation result message - %s", err.Error())
		}

		// Server responds with version
		_, versionMessageBody, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a version message - %s", err.Error())
		}

		if versionMessageBody == nil {
			return nil, fmt.Errorf("Could not receive a version message body")
		}

		versionMessage := message.IRODSMessageVersion{}
		err = versionMessage.FromMessageBody(versionMessageBody)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a version message body")
		}

		if negotiationResult == types.CSNegotiationUseSSL {
			err := conn.sslStartup()
			if err != nil {
				return nil, fmt.Errorf("Could not start up SSL - %s", err.Error())
			}
		}

		return versionMessage.GetVersion(), nil
	}

	return nil, fmt.Errorf("Unknown response message - %s", negotiationMessageBody.Type)
}

func (conn *IRODSConnection) connectWithoutCSNegotiation() (*types.IRODSVersion, error) {
	// No client-server negotiation
	// Send a startup message
	util.LogInfo("Start up a new connection")
	startupMessage := message.NewIRODSMessageStartupPack(conn.Account, conn.ApplicationName, false)
	startupMessageBody, err := startupMessage.GetMessageBody()
	if err != nil {
		return nil, fmt.Errorf("Could not make a startup message - %s", err.Error())
	}

	err = conn.SendMessage(nil, startupMessageBody)
	if err != nil {
		return nil, fmt.Errorf("Could not send a startup message - %s", err.Error())
	}

	// Server responds with version
	_, versionMessageBody, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("Could not receive a version message - %s", err.Error())
	}

	if versionMessageBody == nil {
		return nil, fmt.Errorf("Could not receive a version message body")
	}

	versionMessage := message.IRODSMessageVersion{}
	err = versionMessage.FromMessageBody(versionMessageBody)
	if err != nil {
		return nil, fmt.Errorf("Could not receive a version message body")
	}

	return versionMessage.GetVersion(), nil
}

func (conn *IRODSConnection) sslStartup() error {
	util.LogInfo("Start up SSL")

	irodsSSLConfig := conn.Account.SSLConfiguration
	if irodsSSLConfig == nil {
		return fmt.Errorf("SSL Configuration is not set")
	}

	caCertPool := x509.NewCertPool()
	caCert, err := irodsSSLConfig.ReadCACert()
	if err == nil {
		caCertPool.AppendCertsFromPEM(caCert)
	}

	sslConf := &tls.Config{
		RootCAs: caCertPool,
	}

	// Create a side connection using the existing socket
	sslSocket := tls.Client(conn.socket, sslConf)

	err = sslSocket.Handshake()
	if err != nil {
		return fmt.Errorf("SSL Handshake error - %s", err.Error())
	}

	// from now on use ssl socket
	conn.socket = sslSocket

	// Generate a key (shared secret)
	encryptionKey := make([]byte, irodsSSLConfig.EncryptionKeySize)
	_, err = rand.Read(encryptionKey)
	if err != nil {
		return fmt.Errorf("Could not generate a shared secret - %s", err.Error())
	}

	// Send a ssl setting
	sslSettingMessage := message.NewIRODSMessageSSLSettings(irodsSSLConfig.EncryptionAlgorithm, irodsSSLConfig.EncryptionKeySize, irodsSSLConfig.SaltSize, irodsSSLConfig.HashRounds)
	sslSettingMessageHeader, err := sslSettingMessage.GetMessageHeader()
	if err != nil {
		return fmt.Errorf("Could not make a ssl setting message - %s", err.Error())
	}

	err = conn.SendMessage(sslSettingMessageHeader, nil)
	if err != nil {
		return fmt.Errorf("Could not send a ssl setting message - %s", err.Error())
	}

	// Send a shared secret
	sslSharedSecretMessage := message.NewIRODSMessageSSLSharedSecret(encryptionKey)
	sslSharedSecretMessageBody, err := sslSharedSecretMessage.GetMessageBody()
	if err != nil {
		return fmt.Errorf("Could not make a ssl shared secret message - %s", err.Error())
	}

	err = conn.SendMessage(nil, sslSharedSecretMessageBody)
	if err != nil {
		return fmt.Errorf("Could not send a ssl shared secret message - %s", err.Error())
	}

	return nil
}

func (conn *IRODSConnection) loginNative() error {
	util.LogInfo("Logging in using native authentication method")

	// authenticate
	authRequestMessage := message.NewIRODSMessageAuthRequest()
	authRequestMessageHeader, err := authRequestMessage.GetMessageHeader()
	if err != nil {
		return fmt.Errorf("Could not make a login request message - %s", err.Error())
	}

	err = conn.SendMessage(authRequestMessageHeader, nil)
	if err != nil {
		return fmt.Errorf("Could not send a login request message - %s", err.Error())
	}

	// challenge
	_, authChallengeMessageBody, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("Could not receive an authentication challenge message - %s", err.Error())
	}

	authChallengeMessage := message.IRODSMessageAuthChallenge{}
	err = authChallengeMessage.FromMessageBody(authChallengeMessageBody)
	if err != nil {
		return fmt.Errorf("Could not receive an authentication challenge message body")
	}

	challenge, err := base64.StdEncoding.DecodeString(authChallengeMessage.Challenge)
	if err != nil {
		return fmt.Errorf("Could not decode an authentication challenge")
	}

	paddedPassword := make([]byte, common.MAX_PASSWORD_LENGTH, common.MAX_PASSWORD_LENGTH)
	copy(paddedPassword, []byte(conn.Account.Password))

	m := md5.New()
	m.Write(challenge[:64])
	m.Write(paddedPassword)
	encodedPassword := m.Sum(nil)

	// replace 0x00 to 0x01
	for idx := 0; idx < len(encodedPassword); idx++ {
		if encodedPassword[idx] == 0 {
			encodedPassword[idx] = 1
		}
	}

	b64 := base64.StdEncoding.EncodeToString(encodedPassword[:16])

	authResponseMessage := message.NewIRODSMessageAuthResponse([]byte(b64), conn.Account.ProxyUser)
	authResponseMessageBody, err := authResponseMessage.GetMessageBody()
	if err != nil {
		return fmt.Errorf("Could not make a login response message - %s", err.Error())
	}

	err = conn.SendMessage(nil, authResponseMessageBody)
	if err != nil {
		return fmt.Errorf("Could not send a login response message - %s", err.Error())
	}

	_, authResultMessageBody, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("Could not receive a login result message - %s", err.Error())
	}

	authResultMessage := message.IRODSMessageAuthResult{}
	err = authResultMessage.FromMessageBody(authResultMessageBody)
	if err != nil {
		return fmt.Errorf("Could not receive a login result message body - %s", err.Error())
	}

	err = authResultMessage.CheckError()
	return err
}

func (conn *IRODSConnection) loginGSI() error {
	return nil
}

func (conn *IRODSConnection) loginPAM() error {
	return nil
}

// Disconnect disconnects
func (conn *IRODSConnection) Disconnect() error {
	conn.disconnected = true
	return conn.socket.Close()
}

// Send sends data
func (conn *IRODSConnection) Send(buffer []byte, size int) error {
	// use sslSocket
	if conn.Timeout > 0 {
		conn.socket.SetWriteDeadline(time.Now().Add(conn.Timeout))
	}

	err := util.WriteBytes(conn.socket, buffer, size)
	if err != nil {
		util.LogError("Unable to send data. " +
			"Connection to remote host may have closed. " +
			"Releasing connection from pool.")
		conn.release(true)
		return fmt.Errorf("Unable to send data - %s", err.Error())
	}
	return nil
}

// Recv receives a message
func (conn *IRODSConnection) Recv(buffer []byte, size int) (int, error) {
	if conn.Timeout > 0 {
		conn.socket.SetReadDeadline(time.Now().Add(conn.Timeout))
	}

	readLen, err := util.ReadBytes(conn.socket, buffer, size)
	if err != nil {
		util.LogError("Unable to receive data. " +
			"Connection to remote host may have closed. " +
			"Releasing connection from pool.")
		conn.release(true)
		return readLen, fmt.Errorf("Unable to receive data - %s", err.Error())
	}
	return readLen, nil
}

// SendMessage makes the message into bytes
func (conn *IRODSConnection) SendMessage(header *message.IRODSMessageHeader, body *message.IRODSMessageBody) error {
	messageBuffer := new(bytes.Buffer)

	if header == nil && body == nil {
		return fmt.Errorf("Header and Body cannot be nil")
	}

	var headerBytes []byte
	var err error

	messageLen := 0
	errorLen := 0
	bsLen := 0

	if body != nil {
		if body.Message != nil {
			messageLen = len(body.Message)
		}

		if body.Error != nil {
			errorLen = len(body.Error)
		}

		if body.Bs != nil {
			bsLen = len(body.Bs)
		}

		if header == nil {
			h := message.MakeIRODSMessageHeader(body.Type, uint32(messageLen), uint32(errorLen), uint32(bsLen), body.IntInfo)
			headerBytes, err = h.GetBytes()
			if err != nil {
				return err
			}
		}
	}

	if header != nil {
		headerBytes, err = header.GetBytes()
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

	if body != nil {
		bodyBytes, err := body.GetBytes()
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

// ReadMessageHeader reads data from the given connection and returns iRODSMessageHeader
func (conn *IRODSConnection) ReadMessageHeader() (*message.IRODSMessageHeader, error) {
	// read header size
	headerLenBuffer := make([]byte, 4)
	readLen, err := conn.Recv(headerLenBuffer, 4)
	if readLen != 4 {
		return nil, fmt.Errorf("Could not read header size")
	}
	if err != nil {
		return nil, fmt.Errorf("Could not read header size - %s", err.Error())
	}

	headerSize := binary.BigEndian.Uint32(headerLenBuffer)
	if headerSize <= 0 {
		return nil, fmt.Errorf("Invalid header size returned - len = %d", headerSize)
	}

	// read header
	headerBuffer := make([]byte, headerSize)
	readLen, err = conn.Recv(headerBuffer, int(headerSize))
	if err != nil {
		return nil, fmt.Errorf("Could not read header - %s", err.Error())
	}
	if readLen != int(headerSize) {
		return nil, fmt.Errorf("Could not read header fully - %d requested but %d read", headerSize, readLen)
	}

	header := message.IRODSMessageHeader{}
	err = header.FromBytes(headerBuffer)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

// ReadMessage reads data from the given socket and returns IRODSMessage
func (conn *IRODSConnection) ReadMessage() (*message.IRODSMessageHeader, *message.IRODSMessageBody, error) {
	header, err := conn.ReadMessageHeader()
	if err != nil {
		return nil, nil, err
	}

	// read body
	bodyLen := header.MessageLen + header.ErrorLen + header.BsLen
	bodyBuffer := make([]byte, bodyLen)

	readLen, err := conn.Recv(bodyBuffer, int(bodyLen))
	if err != nil {
		return nil, nil, fmt.Errorf("Could not read body - %s", err.Error())
	}
	if readLen != int(bodyLen) {
		return nil, nil, fmt.Errorf("Could not read body fully - %d requested but %d read", bodyLen, readLen)
	}

	body := message.IRODSMessageBody{}
	err = body.FromBytes(header, bodyBuffer)
	if err != nil {
		return nil, nil, err
	}

	body.Type = header.Type
	body.IntInfo = header.IntInfo

	return header, &body, nil
}

func (conn *IRODSConnection) release(val bool) {
}
