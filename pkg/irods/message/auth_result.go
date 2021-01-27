package message

import "github.com/iychoi/go-irodsclient/pkg/irods/common"

// IRODSMessageAuthResult stores authentication result
type IRODSMessageAuthResult struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageAuthResult) CheckError() error {
	if msg.Result < 0 {
		return common.MakeIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessageBody returns struct from IRODSMessageBody
func (msg *IRODSMessageAuthResult) FromMessageBody(messageBody *IRODSMessageBody) error {
	msg.Result = int(messageBody.IntInfo)
	return nil
}
