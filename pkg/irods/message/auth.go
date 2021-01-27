package message

import (
	"encoding/xml"
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

// ToXML returns XML byte array
func (out *IRODSMessageAuthPluginOut) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(out)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (out *IRODSMessageAuthPluginOut) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, out)
	return err
}

// IRODSMessagePluginAuthMessage stores auth plugin message
type IRODSMessagePluginAuthMessage struct {
	XMLName    xml.Name `xml:"authPlugReqInp_PI"`
	AuthScheme string   `xml:"auth_scheme"`
	Context    string   `xml:"context_"`
}

// NewIRODSMessagePluginAuthMessage creates a IRODSMessagePluginAuthMessage message
func NewIRODSMessagePluginAuthMessage(authScheme string, context string) *IRODSMessagePluginAuthMessage {
	return &IRODSMessagePluginAuthMessage{
		AuthScheme: authScheme,
		Context:    context,
	}
}

// ToXML returns XML byte array
func (message *IRODSMessagePluginAuthMessage) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(message)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (message *IRODSMessagePluginAuthMessage) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, message)
	return err
}
