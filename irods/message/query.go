package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// IRODSMessageQuery stores query
type IRODSMessageQuery struct {
	XMLName           xml.Name             `xml:"GenQueryInp_PI"`
	MaxRows           int                  `xml:"maxRows"`
	ContinueIndex     int                  `xml:"continueInx"`       // 1 for continueing, 0 for end
	PartialStartIndex int                  `xml:"partialStartIndex"` // unknown
	Options           int                  `xml:"options"`
	KeyVals           IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
	Selects           IRODSMessageIIKeyVal `xml:"InxIvalPair_PI"`
	Conditions        IRODSMessageISKeyVal `xml:"InxValPair_PI"`
}

// NewIRODSMessageQuery creates a IRODSMessageQuery message
func NewIRODSMessageQuery(maxRows int, continueIndex int, partialStartIndex int, options int) *IRODSMessageQuery {
	return &IRODSMessageQuery{
		MaxRows:           maxRows,
		ContinueIndex:     continueIndex,
		PartialStartIndex: partialStartIndex,
		Options:           options,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
		Selects: IRODSMessageIIKeyVal{
			Length: 0,
		},
		Conditions: IRODSMessageISKeyVal{
			Length: 0,
		},
	}
}

// AddSelect adds a column to select
func (msg *IRODSMessageQuery) AddSelect(key common.ICATColumnNumber, val int) {
	msg.Selects.Add(int(key), val)
}

// AddCondition adds a condition
func (msg *IRODSMessageQuery) AddCondition(key common.ICATColumnNumber, val string) {
	escapedVal := util.EscapeXMLSpecialChars(val)
	msg.Conditions.Add(int(key), escapedVal)
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageQuery) AddKeyVal(key common.KeyWord, val string) {
	escapedVal := util.EscapeXMLSpecialChars(val)
	msg.KeyVals.Add(string(key), escapedVal)
}

// GetBytes returns byte array
func (msg *IRODSMessageQuery) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQuery) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageQuery) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.GEN_QUERY_AN),
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
