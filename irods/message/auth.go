package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAuthPluginOut stores auth plugin info
type IRODSMessageAuthPluginOut struct {
	XMLName xml.Name `xml:"authPlugReqOut_PI"`
	Result  string   `xml:"result_"`
}

// NewIRODSMessageAuthPluginOut creates a IRODSMessageAuthPluginOut message
func NewIRODSMessageAuthPluginOut(result string) *IRODSMessageAuthPluginOut {
	return &IRODSMessageAuthPluginOut{
		Result: result,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthPluginOut) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthPluginOut) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageAuthPluginOut) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REPLY_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.AUTH_PLUG_RESP_AN),
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthPluginOut) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}

// IRODSMessagePluginAuthMessage stores auth plugin message
type IRODSMessagePluginAuthMessage struct {
	XMLName    xml.Name `xml:"authPlugReqInp_PI"`
	AuthScheme string   `xml:"auth_scheme_"`
	Context    string   `xml:"context_"`
}

// NewIRODSMessagePluginAuthMessage creates a IRODSMessagePluginAuthMessage message
func NewIRODSMessagePluginAuthMessage(authScheme string, context string) *IRODSMessagePluginAuthMessage {
	return &IRODSMessagePluginAuthMessage{
		AuthScheme: authScheme,
		Context:    context,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessagePluginAuthMessage) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessagePluginAuthMessage) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessagePluginAuthMessage) GetMessage() (*IRODSMessage, error) {
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessagePluginAuthMessage) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
