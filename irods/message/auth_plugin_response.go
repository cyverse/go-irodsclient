package message

import (
	"encoding/xml"
	"fmt"
)

// IRODSMessageAuthPluginResponse stores auth plugin info
type IRODSMessageAuthPluginResponse struct {
	XMLName xml.Name `xml:"authPlugReqOut_PI"`
	Result  string   `xml:"result_"`
}

// NewIRODSMessageAuthPluginResponse creates a IRODSMessageAuthPluginResponse
func NewIRODSMessageAuthPluginResponse(result string) *IRODSMessageAuthPluginResponse {
	return &IRODSMessageAuthPluginResponse{
		Result: result,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthPluginResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthPluginResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthPluginResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
