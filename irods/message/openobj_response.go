package message

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageOpenobjResponse stores data object open response
type IRODSMessageOpenobjResponse struct {
	// empty structure
	FileDescriptor int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageOpenobjResponse) CheckError() error {
	if msg.FileDescriptor < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.FileDescriptor))
	}
	return nil
}

// GetFileDescriptor returns file descriptor
func (msg *IRODSMessageOpenobjResponse) GetFileDescriptor() int {
	return msg.FileDescriptor
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageOpenobjResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	msg.FileDescriptor = int(msgIn.Body.IntInfo)
	return nil
}
