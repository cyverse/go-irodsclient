package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageAuthPluginResponse stores auth plugin info
type IRODSMessageAuthPluginResponse struct {
	XMLName           xml.Name `xml:"authPlugReqOut_PI"`
	GeneratedPassword []byte   `xml:"result_"`
	// stores error return
	Result int `xml:"-"`
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageAuthPluginResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthPluginResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthPluginResponse) FromBytes(b []byte) error {
	err := xml.Unmarshal(b, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}

	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthPluginResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return errors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return errors.Wrapf(err, "failed to get irods message from message body")
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAuthPluginResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForPasswordResponse()
}
