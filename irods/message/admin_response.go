package message

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAdminResponse stores alter metadata response
type IRODSMessageAdminResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageAdminResponse) CheckError() error {
	if msg.Result < 0 {
		return common.MakeIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAdminResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}
