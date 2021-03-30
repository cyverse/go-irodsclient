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
	requestMessage, err := request.GetMessage()
	if err != nil {
		return fmt.Errorf("Could not make a request message - %v", err)
	}

	// translate xml.Marshal XML into irods-understandable XML (among others, replace &#34; by &quot;)
	err = conn.PreprocessMessage(requestMessage)
	if err != nil {
		return fmt.Errorf("Could not send preprocess message - %v", err)
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

	// translate irods-dialect XML into valid XML
	err = conn.PostprocessMessage(responseMessage)
	if err != nil {
		return fmt.Errorf("Could not send postprocess message - %v", err)
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
