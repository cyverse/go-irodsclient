package message

import (
	"github.com/iychoi/go-irodsclient/pkg/irods/common"
)

// IRODSMessageAuthRequest stores authentication request
type IRODSMessageAuthRequest struct {
	// empty structure
}

// NewIRODSMessageAuthRequest creates a IRODSMessageAuthRequest message
func NewIRODSMessageAuthRequest() *IRODSMessageAuthRequest {
	return &IRODSMessageAuthRequest{}
}

// GetMessageHeader builds a message header
func (msg *IRODSMessageAuthRequest) GetMessageHeader() (*IRODSMessageHeader, error) {
	return &IRODSMessageHeader{
		Type:       RODS_MESSAGE_API_REQ_TYPE,
		MessageLen: 0,
		ErrorLen:   0,
		BsLen:      0,
		IntInfo:    int32(common.AUTH_REQUEST_AN),
	}, nil
}
