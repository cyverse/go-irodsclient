package message

import (
	"encoding/xml"

	"golang.org/x/xerrors"
)

// IRODSMessageAuthPluginResponse stores auth plugin info
type IRODSMessageAuthPluginResponse struct {
	XMLName xml.Name `xml:"authPlugReqOut_PI"`
	Result  string   `xml:"result_"`
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthPluginResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthPluginResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthPluginResponse) FromMessage(msgIn *IRODSMessage) error {
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
func (msg *IRODSMessageAuthPluginResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
