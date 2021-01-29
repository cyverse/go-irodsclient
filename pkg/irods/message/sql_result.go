package message

import "encoding/xml"

// IRODSMessageSQLResult ..
type IRODSMessageSQLResult struct {
	XMLName        xml.Name `xml:"SqlResult_PI"`
	AttributeIndex int      `xml:"attriInx"`
	ResultLen      int      `xml:"reslen"`
	Values         []string `xml:"value,omitempty"`
}
