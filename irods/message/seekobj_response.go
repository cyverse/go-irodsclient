package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageSeekobjResponse stores data object seek response
type IRODSMessageSeekobjResponse struct {
	XMLName xml.Name `xml:"fileLseekOut_PI"`
	Offset  int64    `xml:"offset"`
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSeekobjResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageSeekobjResponse) CheckError() error {
	if msg.Offset < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Offset))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageSeekobjResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
