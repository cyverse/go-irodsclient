package message

import (
	"encoding/xml"
	"fmt"
)

// IRODSMessageQueryResult stores query result
type IRODSMessageQueryResult struct {
	XMLName        xml.Name                `xml:"GenQueryOut_PI"`
	RowCount       int                     `xml:"rowCnt"`
	AttributeCount int                     `xml:"attriCnt"`
	ContinueIndex  int                     `xml:"continueInx"`
	TotalRowCount  int                     `xml:"totalRowCount"`
	SQLResult      []IRODSMessageSQLResult `xml:"SqlResult_PI"`
}

// GetBytes returns byte array
func (msg *IRODSMessageQueryResult) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQueryResult) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageQueryResult) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
