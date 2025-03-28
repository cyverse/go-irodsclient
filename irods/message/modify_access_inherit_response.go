package message

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageModifyAccessInheritResponse stores alter modify access inheritance response
type IRODSMessageModifyAccessInheritResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageModifyAccessInheritResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageModifyAccessInheritResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageModifyAccessInheritResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
