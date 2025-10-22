package message

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageTicketAdminResponse stores ticket admin response
type IRODSMessageTicketAdminResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageTicketAdminResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageTicketAdminResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return errors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageTicketAdminResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
