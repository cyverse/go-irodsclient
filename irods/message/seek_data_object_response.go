package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageSeekDataObjectResponse stores data object seek response
type IRODSMessageSeekDataObjectResponse struct {
	XMLName xml.Name `xml:"fileLseekOut_PI"`
	Offset  int64    `xml:"offset"`
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSeekDataObjectResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageSeekDataObjectResponse) CheckError() error {
	if msg.Offset < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Offset))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageSeekDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
