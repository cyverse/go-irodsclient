package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessagePamAuthRequest stores auth response
type IRODSMessagePamAuthRequest struct {
	XMLName  xml.Name `xml:"pamAuthRequestInp_PI"`
	Username string   `xml:"pamUser"`
	Password string   `xml:"pamPassword"`
	TTL      int      `xml:"timeToLive"`
}

// NewIRODSMessagePamAuthRequest creates a IRODSMessagePamAuthRequest message
func NewIRODSMessagePamAuthRequest(username, password string, ttl int) *IRODSMessagePamAuthRequest {
	return &IRODSMessagePamAuthRequest{
		Username: username,
		Password: password,
		TTL:      ttl,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessagePamAuthRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessagePamAuthRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessagePamAuthRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.PAM_AUTH_REQUEST_AN),
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
