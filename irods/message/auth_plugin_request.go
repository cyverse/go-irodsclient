package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAuthPluginRequest stores auth plugin request
type IRODSMessageAuthPluginRequest struct {
	XMLName    xml.Name `xml:"authPlugReqInp_PI"`
	AuthScheme string   `xml:"auth_scheme_"`
	Context    string   `xml:"context_"`
}

// NewIRODSMessageAuthPluginRequest creates a IRODSMessageAuthPluginRequest
func NewIRODSMessageAuthPluginRequest(authScheme string, context string) *IRODSMessageAuthPluginRequest {
	return &IRODSMessageAuthPluginRequest{
		AuthScheme: authScheme,
		Context:    context,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthPluginRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthPluginRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageAuthPluginRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.AUTH_PLUG_REQ_AN),
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
