package message

import (
	"encoding/xml"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
)

// IRODSMessageAuthResponse stores auth response
type IRODSMessageAuthResponse struct {
	XMLName  xml.Name `xml:"authResponseInp_PI"`
	Response []byte   `xml:"response"`
	Username string   `xml:"username"`
}

// NewIRODSMessageAuthResponse creates a IRODSMessageAuthResponse message
func NewIRODSMessageAuthResponse(response []byte, username string) *IRODSMessageAuthResponse {
	return &IRODSMessageAuthResponse{
		Response: response,
		Username: username,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessageBody builds a message body
func (msg *IRODSMessageAuthResponse) GetMessageBody() (*IRODSMessageBody, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	return &IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.AUTH_RESPONSE_AN),
	}, nil
}
