package message

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageReplicateDataObjectResponse stores data object replication response
type IRODSMessageReplicateDataObjectResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageReplicateDataObjectResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageReplicateDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageReplicateDataObjectResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
