package message

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageModAccessResponse stores alter metadata response
type IRODSMessageModAccessResponse struct {
	// empty structure
	Result int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageModAccessResponse) CheckError() error {
	if msg.Result < 0 {
		return common.MakeIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageModAccessResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	msg.Result = int(msgIn.Body.IntInfo)
	return nil
}
