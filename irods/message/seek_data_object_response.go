package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageSeekDataObjectResponse stores data object seek response
type IRODSMessageSeekDataObjectResponse struct {
	XMLName xml.Name `xml:"fileLseekOut_PI"`
	Offset  int64    `xml:"offset"`
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSeekDataObjectResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageSeekDataObjectResponse) CheckError() error {
	if msg.Offset < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Offset))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageSeekDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return xerrors.Errorf("failed to get irods message from message body: %w", err)
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageSeekDataObjectResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
