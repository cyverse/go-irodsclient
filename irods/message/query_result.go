package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageQueryResult stores query result
type IRODSMessageQueryResult struct {
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
func (msg *IRODSMessageQueryResult) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageQueryResult) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQueryResult) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageQueryResult) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	msg.Result = int(msgIn.Body.IntInfo)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageQueryResult) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REPLY_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(msg.Result),
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
