package message

import (
	"encoding/xml"

	"golang.org/x/xerrors"
)

// IRODSMessagePamAuthResponse stores auth challenge
type IRODSMessagePamAuthResponse struct {
	XMLName           xml.Name `xml:"pamAuthRequestOut_PI"`
	GeneratedPassword string   `xml:"irodsPamPassword"`
}

// GetBytes returns byte array
func (msg *IRODSMessagePamAuthResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessagePamAuthResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessagePamAuthResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return xerrors.Errorf("failed to get irods message from message body")
	}
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessagePamAuthResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
