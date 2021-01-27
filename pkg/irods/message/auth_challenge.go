package message

import (
	"encoding/xml"
)

// IRODSMessageAuthChallenge stores auth challenge
type IRODSMessageAuthChallenge struct {
	XMLName   xml.Name `xml:"authRequestOut_PI"`
	Challenge string   `xml:"challenge"`
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthChallenge) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthChallenge) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessageBody returns struct from IRODSMessageBody
func (msg *IRODSMessageAuthChallenge) FromMessageBody(messageBody *IRODSMessageBody) error {
	err := msg.FromBytes(messageBody.Message)
	return err
}
