package message

import (
	"fmt"

	"github.com/iychoi/go-irodsclient/irods/common"
)

// IRODSMessageMkcolResponse stores collection creation response
type IRODSMessageMkcolResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageMkcolResponse) CheckError() error {
	if msg.Result < 0 {
		return common.MakeIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageMkcolResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}
