package connection

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
	log "github.com/sirupsen/logrus"
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
// bsBuffer is optional
func (conn *IRODSConnection) Request(request Request, response Response, bsBuffer []byte) error {
	return conn.RequestWithTrackerCallBack(request, response, bsBuffer, nil, nil)
}

// RequestWithTrackerCallBack sends a request and expects a response.
// bsBuffer is optional
func (conn *IRODSConnection) RequestWithTrackerCallBack(request Request, response Response, bsBuffer []byte, reqCallback common.TrackerCallBack, resCallback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestWithTrackerCallBack",
	})

	requestMessage, err := request.GetMessage()
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not make a request message")
	}

	// translate xml.Marshal XML into irods-understandable XML (among others, replace &#34; by &quot;)
	err = conn.PreprocessMessage(requestMessage, false)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send preprocess message")
	}

	err = conn.SendMessageWithTrackerCallBack(requestMessage, reqCallback)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send a request message")
	}

	// Server responds with results
	// external bs buffer
	responseMessage, err := conn.ReadMessageWithTrackerCallBack(bsBuffer, resCallback)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not receive a response message")
	}

	// translate irods-dialect XML into valid XML
	err = conn.PostprocessMessage(responseMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send postprocess message")
	}

	err = response.FromMessage(responseMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not parse a response message")
	}

	return nil
}

// RequestForPassword sends a request and expects a response. XML escape only for '&'
// bsBuffer is optional
func (conn *IRODSConnection) RequestForPassword(request Request, response Response, bsBuffer []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestForPassword",
	})

	requestMessage, err := request.GetMessage()
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not make a request message")
	}

	// translate xml.Marshal XML into irods-understandable XML (among others, replace &#34; by &quot;)
	err = conn.PreprocessMessage(requestMessage, true)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send preprocess message")
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send a request message")
	}

	// Server responds with results
	// external bs buffer
	responseMessage, err := conn.ReadMessage(bsBuffer)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not receive a response message")
	}

	// translate irods-dialect XML into valid XML
	err = conn.PostprocessMessage(responseMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send postprocess message")
	}

	err = response.FromMessage(responseMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not parse a response message")
	}

	return nil
}

// RequestWithoutResponse sends a request but does not wait for a response.
func (conn *IRODSConnection) RequestWithoutResponse(request Request) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestWithoutResponse",
	})

	requestMessage, err := request.GetMessage()
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not make a request message")
	}

	// translate xml.Marshal XML into irods-understandable XML (among others, replace &#34; by &quot;)
	err = conn.PreprocessMessage(requestMessage, false)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send preprocess message")
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send a request message")
	}

	return nil
}

// RequestWithoutResponseNoXML sends a request but does not wait for a response.
func (conn *IRODSConnection) RequestWithoutResponseNoXML(request Request) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestWithoutResponseNoXML",
	})

	requestMessage, err := request.GetMessage()
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not make a request message")
	}

	err = conn.SendMessage(requestMessage)
	if err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return fmt.Errorf("could not send a request message")
	}

	return nil
}

// RequestAndCheck sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheck(request Request, response CheckErrorResponse, bsBuffer []byte) error {
	return conn.RequestAndCheckWithTrackerCallBack(request, response, bsBuffer, nil, nil)
}

// RequestAndCheckWithCallBack sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheckWithTrackerCallBack(request Request, response CheckErrorResponse, bsBuffer []byte, reqCallback common.TrackerCallBack, resCallback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestAndCheckWithCallBack",
	})

	if err := conn.RequestWithTrackerCallBack(request, response, bsBuffer, reqCallback, resCallback); err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return err
	}

	return response.CheckError()
}

// RequestAndCheckForPassword sends a request and expects a CheckErrorResponse, on which the error is already checked.
// Only escape '&'
func (conn *IRODSConnection) RequestAndCheckForPassword(request Request, response CheckErrorResponse, bsBuffer []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "connection",
		"struct":   "IRODSConnection",
		"function": "RequestAndCheckForPassword",
	})

	if err := conn.RequestForPassword(request, response, bsBuffer); err != nil {
		logger.Error(err)
		if conn.metrics != nil {
			conn.metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return err
	}

	return response.CheckError()
}
