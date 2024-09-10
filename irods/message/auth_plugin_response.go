package message

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"

	"golang.org/x/xerrors"
)

// IRODSMessageAuthPluginResponse stores auth plugin info
type IRODSMessageAuthPluginResponse struct {
	XMLName xml.Name `xml:"authPlugReqOut_PI"`
	Result  []byte   `xml:"result_"`
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
func (msg *IRODSMessageAuthPluginResponse) FromBytes(b []byte) error {
	err := xml.Unmarshal(b, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}

	// handle escape
	// GetXMLCorrectorForPasswordResponse() converts non-ascii bytes to hex string
	buf := b
	unescaped := &bytes.Buffer{}

	for len(buf) > 0 {
		if bytes.HasPrefix(buf, []byte("0x")) {
			bhex := buf[2:4]
			original, err := hex.DecodeString(string(bhex))
			if err != nil {
				return err
			}

			unescaped.Write(original)
			buf = buf[4:]
		} else {
			unescaped.Write(buf[:1])
			buf = buf[1:]
		}
	}

	msg.Result = unescaped.Bytes()
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthPluginResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return xerrors.Errorf("failed to get irods message from message body: %w", err)
	}
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAuthPluginResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForPasswordResponse()
}
