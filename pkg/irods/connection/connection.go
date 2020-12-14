package connection

import (
	"fmt"
	"net"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/util"

	"github.com/iychoi/go-irodsclient/pkg/irods/api"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

// IRODSConnection connects to iRODS
type IRODSConnection struct {
	Account         *types.IRODSAccount
	Timeout         time.Duration
	ApplicationName string
	// internal
	disconnected bool
	socket       net.Conn
}

// NewIRODSConnection create a IRODSConnection
func NewIRODSConnection(account *types.IRODSAccount, timeout time.Duration, applicationName string) *IRODSConnection {
	return &IRODSConnection{
		Account:         account,
		Timeout:         timeout,
		ApplicationName: applicationName,
	}
}

func (irodsConn *IRODSConnection) requiresCSNegotiation() bool {
	if irodsConn.Account.ClientServerNegotiation == api.REQUEST_NEGOTIATION {
		return true
	}
	return false
}

// Connect connects to iRODS
func (irodsConn *IRODSConnection) Connect() error {
	server := fmt.Sprintf("%s:%d", irodsConn.Account.Host, irodsConn.Account.Port)
	socket, err := net.Dial("tcp", server)
	if err != nil {
		return fmt.Errorf("Could not connect to specified host and port (%s:%d) - %s", irodsConn.Account.Host, irodsConn.Account.Port, err.Error())
	}

	irodsConn.disconnected = false
	irodsConn.socket = socket

	startupMessage := NewIRODSMessageStartupPackWithAppName(irodsConn.Account, irodsConn.ApplicationName)

	if irodsConn.requiresCSNegotiation() {
		// client-server negotiation
		return fmt.Errorf("Not implemented yet")
	} else {
		// No client-server negotiation
		// Send startup pack without negotiation request
		msg := NewIRODSMessage(startupMessage)

		err = irodsConn.Send(msg)
		if err != nil {
			return fmt.Errorf("Could not send a message - %s", err.Error())
		}

		// Server responds with version
		_, err := irodsConn.Recv()
		if err != nil {
			return fmt.Errorf("Could not receive a message - %s", err.Error())
		}

		return nil
		// Done
		//return version_msg.get_main_message(VersionResponse)
	}

}

// Send sends a message
func (irodsConn *IRODSConnection) Send(message *IRODSMessage) error {
	if irodsConn.Timeout > 0 {
		irodsConn.socket.SetWriteDeadline(time.Now().Add(irodsConn.Timeout))
	}

	err := WriteIRODSMessage(irodsConn.socket, message)
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
func (irodsConn *IRODSConnection) Recv() (*IRODSMessage, error) {
	if irodsConn.Timeout > 0 {
		irodsConn.socket.SetReadDeadline(time.Now().Add(irodsConn.Timeout))
	}

	message, err := ReadIRODSMessage(irodsConn.socket)
	if err != nil {
		util.LogError("Unable to receive a message. " +
			"Connection to remote host may have closed. " +
			"Releasing connection from pool.")
		irodsConn.release(true)
		return nil, fmt.Errorf("Unable to receive a message - %s", err.Error())
	}

	return message, nil
}

func (irodsConn *IRODSConnection) release(val bool) {

}
