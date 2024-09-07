package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageGetDataObjectResponse stores file get response
type IRODSMessageGetDataObjectResponse IRODSMessagePortalResponse

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageGetDataObjectResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDataObjectResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageGetDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return xerrors.Errorf("failed to get irods message from message body: %w", err)
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetDataObjectResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
