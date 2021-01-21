package connection

import (
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
func (irodsConn *IRODSConnection) GetVersion() *types.IRODSVersion {
	return irodsConn.serverVersion
}

func (irodsConn *IRODSConnection) requiresCSNegotiation() bool {
	return irodsConn.Account.ClientServerNegotiation
}

// Connect connects to iRODS
func (irodsConn *IRODSConnection) Connect() error {
	irodsConn.disconnected = true

	server := fmt.Sprintf("%s:%d", irodsConn.Account.Host, irodsConn.Account.Port)
	socket, err := net.Dial("tcp", server)
	if err != nil {
		return fmt.Errorf("Could not connect to specified host and port (%s:%d) - %s", irodsConn.Account.Host, irodsConn.Account.Port, err.Error())
	}

	irodsConn.socket = socket
	var irodsVersion *types.IRODSVersion
	if irodsConn.requiresCSNegotiation() {
		// client-server negotiation
		irodsVersion, err = irodsConn.connectWithCSNegotiation()
	} else {
		// No client-server negotiation
		irodsVersion, err = irodsConn.connectWithoutCSNegotiation()
	}

	if err != nil {
		_ = irodsConn.Disconnect()
		irodsConn.disconnected = false
		return err
	}

	irodsConn.serverVersion = irodsVersion
	return nil
}

func (irodsConn *IRODSConnection) connectWithCSNegotiation() (*types.IRODSVersion, error) {
	// Get client negotiation policy
	clientPolicy := types.CSNegotiationRequireTCP
	if len(irodsConn.Account.CSNegotiationPolicy) > 0 {
		clientPolicy = irodsConn.Account.CSNegotiationPolicy
	}

	optionString := fmt.Sprintf("%s;%s", irodsConn.ApplicationName, message.REQUEST_NEGOTIATION)
	startupMessage := message.NewIRODSMessageStartupPackWithOption(irodsConn.Account, optionString)

	// Send startup pack with negotiation request
	msgStartup := message.NewIRODSMessage(startupMessage)

	err := irodsConn.Send(msgStartup)
	if err != nil {
		return nil, fmt.Errorf("Could not send a message - %s", err.Error())
	}

	// Server responds with version
	msgResponse, err := irodsConn.Recv()
	if err != nil {
		return nil, fmt.Errorf("Could not receive a message - %s", err.Error())
	}

	var messageVersion message.IRODSMessageVersion

	if msgResponse.Type == message.RODS_CS_NEG_TYPE {
		// Server responds with its own negotiation policy
		msgNegotiation := msgResponse
		messageNegotiation := msgNegotiation.Message.(message.IRODSMessageCSNegotiation)

		serverPolicy, err := types.GetCSNegotiationRequire(messageNegotiation.Result)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse server policy - %s", messageNegotiation.Result)
		}

		// Perform the negotiation
		negotiationResult, status := performCSNegotiation(clientPolicy, serverPolicy)

		// Send negotiation result to server
		negotiationResultString := fmt.Sprintf("%s=%s;", message.CS_NEG_RESULT_KW, string(negotiationResult))
		negotiationResultMessage := message.NewIRODSMessageCSNegotiation(status, negotiationResultString)

		msgNegotiationResult := message.NewIRODSMessage(negotiationResultMessage)
		err = irodsConn.Send(msgNegotiationResult)
		if err != nil {
			return nil, fmt.Errorf("Could not send a message - %s", err.Error())
		}

		// If negotiation failed we're done
		if negotiationResult == types.CSNegotiationFailure {
			return nil, fmt.Errorf("Client-Server negotiation failed: %s, %s", string(clientPolicy), string(serverPolicy))
		}

		// Server responds with version
		msgVersion, err := irodsConn.Recv()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a message - %s", err.Error())
		}

		if negotiationResult == types.CSNegotiationUseSSL {
			err := irodsConn.sslStartup()
			if err != nil {
				return nil, fmt.Errorf("Could not start up SSL - %s", err.Error())
			}
		}

		messageVersion = msgVersion.Message.(message.IRODSMessageVersion)
	} else {
		util.LogDebug("Negotiation did not happen. Server did not responde for the negotiation request.")
		// Server responds with version
		messageVersion = msgResponse.Message.(message.IRODSMessageVersion)
	}

	return messageVersion.ConvertToIRODSVersion(), nil
}

func (irodsConn *IRODSConnection) connectWithoutCSNegotiation() (*types.IRODSVersion, error) {
	// No client-server negotiation
	// Send startup pack without negotiation request
	optionString := irodsConn.ApplicationName
	startupMessage := message.NewIRODSMessageStartupPackWithOption(irodsConn.Account, optionString)

	// Send startup pack
	msgStartup := message.NewIRODSMessage(startupMessage)

	err := irodsConn.Send(msgStartup)
	if err != nil {
		return nil, fmt.Errorf("Could not send a message - %s", err.Error())
	}

	// Server responds with version
	msgVersion, err := irodsConn.Recv()
	if err != nil {
		return nil, fmt.Errorf("Could not receive a message - %s", err.Error())
	}

	messageVersion := msgVersion.Message.(message.IRODSMessageVersion)
	return messageVersion.ConvertToIRODSVersion(), nil
}

func (irodsConn *IRODSConnection) sslStartup() error {
	return nil
}

// Disconnect disconnects
func (irodsConn *IRODSConnection) Disconnect() error {
	return nil
}

// Send sends a message
func (irodsConn *IRODSConnection) Send(msg *message.IRODSMessage) error {
	if irodsConn.Timeout > 0 {
		irodsConn.socket.SetWriteDeadline(time.Now().Add(irodsConn.Timeout))
	}

	err := message.WriteIRODSMessage(irodsConn.socket, msg)
	if err != nil {
		util.LogError("Unable to send a message. " +
			"Connection to remote host may have closed. " +
			"Releasing connection from pool.")
		irodsConn.release(true)
		return fmt.Errorf("Unable to send a message - %s", err.Error())
	}

	return nil
}

// Recv receives a message
func (irodsConn *IRODSConnection) Recv() (*message.IRODSMessage, error) {
	if irodsConn.Timeout > 0 {
		irodsConn.socket.SetReadDeadline(time.Now().Add(irodsConn.Timeout))
	}

	msg, err := message.ReadIRODSMessage(irodsConn.socket)
	if err != nil {
		util.LogError("Unable to receive a message. " +
			"Connection to remote host may have closed. " +
			"Releasing connection from pool.")
		irodsConn.release(true)
		return nil, fmt.Errorf("Unable to receive a message - %s", err.Error())
	}

	if msg.IntInfo < 0 {
		messageError := string(msg.Error)
		err := common.MakeIRODSError(common.ErrorCode(msg.IntInfo), messageError)
		return nil, err
	}

	return msg, nil
}

func (irodsConn *IRODSConnection) release(val bool) {

}

func performCSNegotiation(clientRequest types.CSNegotiationRequire, serverRequest types.CSNegotiationRequire) (types.CSNegotiationPolicy, int) {
	if clientRequest == serverRequest {
		if types.CSNegotiationRequireSSL == clientRequest {
			return types.CSNegotiationUseSSL, 1
		} else if types.CSNegotiationRequireTCP == clientRequest {
			return types.CSNegotiationUseTCP, 1
		} else {
			return types.CSNegotiationFailure, 0
		}
	} else {
		return types.CSNegotiationFailure, 0
	}
}
