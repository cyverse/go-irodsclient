package message

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageReadDataObjectResponse stores data object read response
type IRODSMessageReadDataObjectResponse struct {
	// empty structure
	Result int
	Data   []byte
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageReadDataObjectResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageReadDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	msg.Data = msgIn.Body.Bs
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageReadDataObjectResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
