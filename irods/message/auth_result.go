package message

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageAuthResult stores authentication result
type IRODSMessageAuthResult struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageAuthResult) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAuthResult) GetMessage() (*IRODSMessage, error) {
	msgHeader := IRODSMessageHeader{
		Type:       RODS_MESSAGE_API_REPLY_TYPE,
		MessageLen: 0,
		ErrorLen:   0,
		BsLen:      0,
		IntInfo:    int32(msg.Result),
	}

	return &IRODSMessage{
		Header: &msgHeader,
		Body:   nil,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthResult) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAuthResult) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
