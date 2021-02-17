package message

import (
	"fmt"

	"github.com/iychoi/go-irodsclient/irods/common"
)

// IRODSMessageReadobjResponse stores data object read response
type IRODSMessageReadobjResponse struct {
	// empty structure
	Result int
	Data   []byte
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageReadobjResponse) CheckError() error {
	if msg.Result < 0 {
		return common.MakeIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageReadobjResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	msg.Data = msgIn.Body.Bs
	return nil
}
