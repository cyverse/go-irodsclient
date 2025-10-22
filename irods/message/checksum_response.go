package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageChecksumResponse stores data object checksum response
type IRODSMessageChecksumResponse struct {
	Checksum string `xml:"myStr"`
	// stores error return
	Result int `xml:"-"`
}

type STRI_PI struct {
}

// GetBytes returns byte array
func (msg *IRODSMessageChecksumResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageChecksumResponse) CheckError() error {
	if len(msg.Checksum) == 0 {
		return errors.Errorf("checksum not present in response message")
	}

	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}

	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageChecksumResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageChecksumResponse) FromMessage(msgIn *IRODSMessage) error {
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
func (msg *IRODSMessageChecksumResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
