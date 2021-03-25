package connection

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/message"
)

// A Request to send to irods.
type Request interface {
	GetMessage() (*message.IRODSMessage, error)
}

// A Response to retrieve from irods.
type Response interface {
	FromMessage(*message.IRODSMessage) error
}

// A CheckErrorResponse is a Response on which CheckError can be called.
type CheckErrorResponse interface {
	Response
	CheckError() error
}

// Request sends a request and expects a response.
func (conn *IRODSConnection) Request(request Request, response Response) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	requestMessage, err := request.GetMessage()
	if err != nil {
		return fmt.Errorf("Could not make a request message - %v", err)
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		return fmt.Errorf("Could not send a request message - %v", err)
	}

	// Server responds with results
	responseMessage, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("Could not receive a response message - %v", err)
	}

	err = response.FromMessage(responseMessage)
	if err != nil {
		return fmt.Errorf("Could not parse a response message - %v", err)
	}

	return nil
}

// RequestAndCheck sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheck(request Request, response CheckErrorResponse) error {
	if err := conn.Request(request, response); err != nil {
		return err
	}

	return response.CheckError()
}
