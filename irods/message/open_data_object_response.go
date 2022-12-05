package message

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageOpenDataObjectResponse stores data object open response
type IRODSMessageOpenDataObjectResponse struct {
	// empty structure
	FileDescriptor int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageOpenDataObjectResponse) CheckError() error {
	if msg.FileDescriptor < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.FileDescriptor))
	}
	return nil
}

// GetFileDescriptor returns file descriptor
func (msg *IRODSMessageOpenDataObjectResponse) GetFileDescriptor() int {
	return msg.FileDescriptor
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageOpenDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	msg.FileDescriptor = int(msgIn.Body.IntInfo)
	return nil
}
