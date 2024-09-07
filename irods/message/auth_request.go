package message

import (
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAuthRequest stores authentication request
type IRODSMessageAuthRequest struct {
	// empty structure
}

// NewIRODSMessageAuthRequest creates a IRODSMessageAuthRequest message
func NewIRODSMessageAuthRequest() *IRODSMessageAuthRequest {
	return &IRODSMessageAuthRequest{}
}

// GetMessage builds a message
func (msg *IRODSMessageAuthRequest) GetMessage() (*IRODSMessage, error) {
	msgHeader := IRODSMessageHeader{
		Type:       RODS_MESSAGE_API_REQ_TYPE,
		MessageLen: 0,
		ErrorLen:   0,
		BsLen:      0,
		IntInfo:    int32(common.AUTH_REQUEST_AN),
	}

	return &IRODSMessage{
		Header: &msgHeader,
		Body:   nil,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthRequest) FromMessage(msgIn *IRODSMessage) error {
	return nil
}

func (msg *IRODSMessageAuthRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
