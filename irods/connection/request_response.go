package connection

import (
	"io"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
	"golang.org/x/xerrors"
)

// Request is an interface for calling iRODS RPC.
type Request interface {
	GetMessage() (*message.IRODSMessage, error)
	GetXMLCorrector() message.XMLCorrector
}

// Response is an interface for response of iRODS RPC Call.
type Response interface {
	FromMessage(message *message.IRODSMessage) error
	GetXMLCorrector() message.XMLCorrector
}

// CheckErrorResponse is a Response on which CheckError can be called.
type CheckErrorResponse interface {
	Response
	CheckError() error
}

// RequestResponsePair is a structure that wraps Request, Response, and other parameters for making iRODS RPC call.
type RequestResponsePair struct {
	Request          Request
	Response         Response
	BsBuffer         []byte // can be null
	Timeout          *RequestResponseTimeout
	RequestCallback  common.TrackerCallBack // can be null
	ResponseCallback common.TrackerCallBack // can be null
	Error            error
}

// RequestResponseTimeout is a structure that contains timeout values for iRODS RPC calls.
type RequestResponseTimeout struct {
	RequestTimeout  time.Duration
	ResponseTimeout time.Duration
}

func (conn *IRODSConnection) useNewXML() bool {
	if conn.serverVersion == nil {
		return true
	}

	return conn.serverVersion.HasHigherVersionThan(4, 2, 9) // new xml is used in 4.2.9
}

// Request sends a request and expects a response.
// bsBuffer is optional
func (conn *IRODSConnection) Request(request Request, response Response, bsBuffer []byte, timeout *RequestResponseTimeout) error {
	return conn.RequestWithTrackerCallBack(request, response, bsBuffer, timeout, nil, nil)
}

// RequestWithTrackerCallBack sends a request and expects a response.
// bsBuffer is optional
func (conn *IRODSConnection) RequestWithTrackerCallBack(request Request, response Response, bsBuffer []byte, timeout *RequestResponseTimeout, reqCallback common.TrackerCallBack, resCallback common.TrackerCallBack) error {
	// set transaction dirty
	conn.SetTransactionDirty(true)

	requestMessage, err := conn.getRequestMessage(request)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return xerrors.Errorf("failed to make a request message: %w", err)
	}

	requestTimeout := time.Duration(0)
	responseTimeout := time.Duration(0)
	if timeout != nil {
		requestTimeout = timeout.RequestTimeout
		responseTimeout = timeout.ResponseTimeout
	}

	err = conn.SendMessageWithTrackerCallBack(requestMessage, requestTimeout, reqCallback)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}

		return xerrors.Errorf("failed to send a request message: %w", err)
	}

	// Server responds with results
	// external bs buffer
	responseMessage, err := conn.ReadMessageWithTrackerCallBack(bsBuffer, responseTimeout, resCallback)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}

		if err == io.EOF {
			return err
		}
		return xerrors.Errorf("failed to receive a response message: %w", err)
	}

	//logger.Debugf("response: %#v", responseMessage)
	//logger.Debugf("response header: %#v", responseMessage.Header)
	//logger.Debugf("response body: %#v", responseMessage.Body)

	err = conn.getResponse(responseMessage, response)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}

		return xerrors.Errorf("failed to parse response message: %w", err)
	}

	return nil
}

// RequestAsyncWithTrackerCallBack sends multiple requests and expects responses.
func (conn *IRODSConnection) RequestAsyncWithTrackerCallBack(rrChan chan RequestResponsePair) chan RequestResponsePair {
	waitResponseChan := make(chan RequestResponsePair, 100)
	outputPair := make(chan RequestResponsePair, 100)

	var lastErr error

	// sender
	go func() {
		for {
			pair, ok := <-rrChan
			if !ok {
				// input closed
				close(waitResponseChan)
				break
			}

			// if errored before? skip
			if lastErr != nil {
				pair.Error = lastErr
				waitResponseChan <- pair
				continue
			}

			requestMessage, err := conn.getRequestMessage(pair.Request)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				lastErr = err
				pair.Error = lastErr
				waitResponseChan <- pair
				continue
			}

			requestTimeout := time.Duration(0)
			if pair.Timeout != nil {
				requestTimeout = pair.Timeout.RequestTimeout
			}

			err = conn.SendMessageWithTrackerCallBack(requestMessage, requestTimeout, pair.RequestCallback)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				lastErr = xerrors.Errorf("failed to send a request message: %w", err)
				pair.Error = lastErr
				waitResponseChan <- pair
				continue
			}

			waitResponseChan <- pair
		}
	}()

	// receiver
	go func() {
		for {
			pair, ok := <-waitResponseChan
			if !ok {
				// input closed
				close(outputPair)
				break
			}

			// if errored before? skip
			if lastErr != nil {
				if pair.Error == nil {
					pair.Error = lastErr
				}
				outputPair <- pair
				continue
			}

			// Server responds with results
			// external bs buffer
			responseTimeout := time.Duration(0)
			if pair.Timeout != nil {
				responseTimeout = pair.Timeout.ResponseTimeout
			}

			responseMessage, err := conn.ReadMessageWithTrackerCallBack(pair.BsBuffer, responseTimeout, pair.ResponseCallback)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				if err == io.EOF {
					lastErr = err
				} else {
					lastErr = xerrors.Errorf("failed to receive a response message: %w", err)
				}

				pair.Error = lastErr
				outputPair <- pair

				continue
			}

			err = conn.getResponse(responseMessage, pair.Response)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				lastErr = xerrors.Errorf("failed to parse response message: %w", err)
				pair.Error = lastErr
				outputPair <- pair
				continue
			}

			outputPair <- pair
		}
	}()

	return outputPair
}

// RequestWithoutResponse sends a request but does not wait for a response.
func (conn *IRODSConnection) RequestWithoutResponse(request Request, timeout *RequestResponseTimeout) error {
	requestMessage, err := conn.getRequestMessage(request)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return err
	}

	requestTimeout := time.Duration(0)
	if timeout != nil {
		requestTimeout = timeout.RequestTimeout
	}

	err = conn.SendMessage(requestMessage, requestTimeout)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return xerrors.Errorf("failed to send a request message: %w", err)
	}

	return nil
}

// RequestAndCheck sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheck(request Request, response CheckErrorResponse, bsBuffer []byte, timeout *RequestResponseTimeout) error {
	return conn.RequestAndCheckWithTrackerCallBack(request, response, bsBuffer, timeout, nil, nil)
}

// RequestAndCheckWithCallBack sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheckWithTrackerCallBack(request Request, response CheckErrorResponse, bsBuffer []byte, timeout *RequestResponseTimeout, reqCallback common.TrackerCallBack, resCallback common.TrackerCallBack) error {
	if err := conn.RequestWithTrackerCallBack(request, response, bsBuffer, timeout, reqCallback, resCallback); err != nil {
		return err
	}

	return response.CheckError()
}

func (conn *IRODSConnection) getRequestMessage(request Request) (*message.IRODSMessage, error) {
	requestMessage, err := request.GetMessage()
	if err != nil {
		return nil, xerrors.Errorf("failed to make a request message: %w", err)
	}

	xmlCorrector := request.GetXMLCorrector()
	if xmlCorrector != nil {
		err := xmlCorrector(requestMessage, conn.useNewXML())
		if err != nil {
			return nil, xerrors.Errorf("failed to corrext XML message: %w", err)
		}
	}

	return requestMessage, nil
}

func (conn *IRODSConnection) getResponse(responseMessage *message.IRODSMessage, response Response) error {
	xmlCorrector := response.GetXMLCorrector()
	if xmlCorrector != nil {
		err := xmlCorrector(responseMessage, conn.useNewXML())
		if err != nil {
			return xerrors.Errorf("failed to corrext XML message: %w", err)
		}
	}

	err := response.FromMessage(responseMessage)
	if err != nil {
		return xerrors.Errorf("failed to parse a response message: %w", err)
	}

	return nil
}

func (conn *IRODSConnection) GetOperationTimeout() *RequestResponseTimeout {
	return &RequestResponseTimeout{
		RequestTimeout:  conn.config.OperationTimeout,
		ResponseTimeout: conn.config.OperationTimeout,
	}
}

func (conn *IRODSConnection) GetLongResponseOperationTimeout() *RequestResponseTimeout {
	return &RequestResponseTimeout{
		RequestTimeout:  conn.config.OperationTimeout,
		ResponseTimeout: conn.config.LongOperationTimeout,
	}
}
