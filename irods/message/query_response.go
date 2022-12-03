package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageQueryResponse stores query result
type IRODSMessageQueryResponse struct {
	XMLName        xml.Name                `xml:"GenQueryOut_PI"`
	RowCount       int                     `xml:"rowCnt"`
	AttributeCount int                     `xml:"attriCnt"`
	ContinueIndex  int                     `xml:"continueInx"`
	TotalRowCount  int                     `xml:"totalRowCount"`
	SQLResult      []IRODSMessageSQLResult `xml:"SqlResult_PI"`

	// stores error result
	Result int `xml:"-"`
}

// GetBytes returns byte array
func (msg *IRODSMessageQueryResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageQueryResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQueryResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageQueryResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	msg.Result = int(msgIn.Body.IntInfo)
	return err
}
