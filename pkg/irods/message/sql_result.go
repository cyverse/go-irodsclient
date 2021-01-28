package message

import "encoding/xml"

// IRODSMessageSQLResult ..
type IRODSMessageSQLResult struct {
	XMLName        xml.Name `xml:"SqlResult_PI"`
	AttributeIndex int      `xml:"attriInx"`
	ResultLen      int      `xml:"reslen"`
	Value          string   `xml:"value,omitempty"`
}

// NewIRODSMessageSQLResult creates a new IRODSMessageSQLResult
func NewIRODSMessageSQLResult(attrIndex int, resultLen int, value string) *IRODSMessageSQLResult {
	return &IRODSMessageSQLResult{
		AttributeIndex: attrIndex,
		ResultLen:      resultLen,
		Value:          value,
	}
}
