package message

import (
	"encoding/xml"
	"fmt"
)

// IRODSMessagePamAuthResponse stores auth challenge
type IRODSMessagePamAuthResponse struct {
	XMLName           xml.Name `xml:"pamAuthRequestOut_PI"`
	GeneratedPassword string   `xml:"irodsPamPassword"`
}

// GetBytes returns byte array
func (msg *IRODSMessagePamAuthResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessagePamAuthResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessagePamAuthResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
