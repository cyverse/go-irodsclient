package connection

import (
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/message"
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
	RequestCallback  common.TransferTrackerCallback // can be null
	ResponseCallback common.TransferTrackerCallback // can be null
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
func (conn *IRODSConnection) RequestWithTrackerCallBack(request Request, response Response, bsBuffer []byte, timeout *RequestResponseTimeout, reqCallback common.TransferTrackerCallback, resCallback common.TransferTrackerCallback) error {
	// set transaction dirty
	conn.SetTransactionDirty(true)

	requestMessage, err := conn.getRequestMessage(request)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}
		return errors.Wrapf(err, "failed to make a request message")
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

		return errors.Wrapf(err, "failed to send a request message")
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
		return errors.Wrapf(err, "failed to receive a response message")
	}

	//logger.Debugf("response: %#v", responseMessage)
	//logger.Debugf("response header: %#v", responseMessage.Header)
	//logger.Debugf("response body: %#v", responseMessage.Body)

	err = conn.getResponse(responseMessage, response)
	if err != nil {
		if conn.config.Metrics != nil {
			conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
		}

		return errors.Wrapf(err, "failed to parse response message")
	}

	return nil
}

// RequestAsyncWithTrackerCallBack sends multiple requests and expects responses.
func (conn *IRODSConnection) RequestAsyncWithTrackerCallBack(rrChan chan RequestResponsePair) chan RequestResponsePair {
	type pendingResponse struct {
		pair RequestResponsePair
		sent bool
	}

	waitResponseChan := make(chan pendingResponse, 100)
	resultChan := make(chan RequestResponsePair, 100)
	outputPair := make(chan RequestResponsePair)
	receiverErrChan := make(chan error, 1)

	// Decouple socket processing from the caller. A caller may submit more than the fixed channel
	// capacity before it starts reading results, so blocking the receiver on output can otherwise
	// stop both the receiver and sender.
	go func() {
		defer close(outputPair)

		queue := []RequestResponsePair{}
		for resultChan != nil || len(queue) > 0 {
			var outputChan chan RequestResponsePair
			var next RequestResponsePair
			if len(queue) > 0 {
				outputChan = outputPair
				next = queue[0]
			}

			select {
			case pair, ok := <-resultChan:
				if !ok {
					resultChan = nil
					continue
				}
				queue = append(queue, pair)
			case outputChan <- next:
				queue = queue[1:]
			}
		}
	}()

	// sender
	go func() {
		var sendErr error

		for {
			pair, ok := <-rrChan
			if !ok {
				// input closed
				close(waitResponseChan)
				break
			}

			if sendErr == nil {
				select {
				case sendErr = <-receiverErrChan:
				default:
				}
			}

			// If the pipeline has already failed, this request was never sent and must not
			// consume a response.
			if sendErr != nil {
				pair.Error = sendErr
				waitResponseChan <- pendingResponse{pair: pair, sent: false}
				continue
			}

			requestMessage, err := conn.getRequestMessage(pair.Request)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				sendErr = err
				pair.Error = sendErr
				waitResponseChan <- pendingResponse{pair: pair, sent: false}
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

				sendErr = errors.Wrapf(err, "failed to send a request message")
				pair.Error = sendErr
				waitResponseChan <- pendingResponse{pair: pair, sent: false}
				continue
			}

			waitResponseChan <- pendingResponse{pair: pair, sent: true}
		}
	}()

	// receiver
	go func() {
		defer close(resultChan)

		var receiveErr error
		for {
			pending, ok := <-waitResponseChan
			if !ok {
				return
			}
			pair := pending.pair

			// Unsent requests never have a corresponding response. After a receive failure,
			// the socket is no longer usable, so all remaining requests fail without reads.
			if !pending.sent || receiveErr != nil {
				if pair.Error == nil {
					pair.Error = receiveErr
				}
				resultChan <- pair
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
					receiveErr = err
				} else {
					receiveErr = errors.Wrapf(err, "failed to receive a response message")
				}
				select {
				case receiverErrChan <- receiveErr:
				default:
				}

				pair.Error = receiveErr
				resultChan <- pair

				continue
			}

			err = conn.getResponse(responseMessage, pair.Response)
			if err != nil {
				if conn.config.Metrics != nil {
					conn.config.Metrics.IncreaseCounterForRequestResponseFailures(1)
				}

				receiveErr = errors.Wrapf(err, "failed to parse response message")
				select {
				case receiverErrChan <- receiveErr:
				default:
				}
				pair.Error = receiveErr
				resultChan <- pair
				continue
			}

			resultChan <- pair
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
		return errors.Wrapf(err, "failed to send a request message")
	}

	return nil
}

// RequestAndCheck sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheck(request Request, response CheckErrorResponse, bsBuffer []byte, timeout *RequestResponseTimeout) error {
	return conn.RequestAndCheckWithTrackerCallBack(request, response, bsBuffer, timeout, nil, nil)
}

// RequestAndCheckWithCallBack sends a request and expects a CheckErrorResponse, on which the error is already checked.
func (conn *IRODSConnection) RequestAndCheckWithTrackerCallBack(request Request, response CheckErrorResponse, bsBuffer []byte, timeout *RequestResponseTimeout, reqCallback common.TransferTrackerCallback, resCallback common.TransferTrackerCallback) error {
	if err := conn.RequestWithTrackerCallBack(request, response, bsBuffer, timeout, reqCallback, resCallback); err != nil {
		return err
	}

	return response.CheckError()
}

func (conn *IRODSConnection) getRequestMessage(request Request) (*message.IRODSMessage, error) {
	requestMessage, err := request.GetMessage()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to make a request message")
	}

	xmlCorrector := request.GetXMLCorrector()
	if xmlCorrector != nil {
		err := xmlCorrector(requestMessage, conn.useNewXML())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to correct XML message")
		}
	}

	return requestMessage, nil
}

func (conn *IRODSConnection) getResponse(responseMessage *message.IRODSMessage, response Response) error {
	xmlCorrector := response.GetXMLCorrector()
	if xmlCorrector != nil {
		err := xmlCorrector(responseMessage, conn.useNewXML())
		if err != nil {
			return errors.Wrapf(err, "failed to correct XML message")
		}
	}

	err := response.FromMessage(responseMessage)
	if err != nil {
		return errors.Wrapf(err, "failed to parse a response message")
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
