package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// IRODSMessageQueryRequest stores query
type IRODSMessageQueryRequest struct {
	XMLName           xml.Name             `xml:"GenQueryInp_PI"`
	MaxRows           int                  `xml:"maxRows"`
	ContinueIndex     int                  `xml:"continueInx"`       // 1 for continuing, 0 for end
	PartialStartIndex int                  `xml:"partialStartIndex"` // unknown
	Options           int                  `xml:"options"`
	KeyVals           IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
	Selects           IRODSMessageIIKeyVal `xml:"InxIvalPair_PI"`
	Conditions        IRODSMessageISKeyVal `xml:"InxValPair_PI"`
}

// NewIRODSMessageQueryRequest creates a IRODSMessageQueryRequest message
func NewIRODSMessageQueryRequest(maxRows int, continueIndex int, partialStartIndex int, options int) *IRODSMessageQueryRequest {
	return &IRODSMessageQueryRequest{
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
func (msg *IRODSMessageQueryRequest) AddSelect(key common.ICATColumnNumber, val int) {
	msg.Selects.Add(int(key), val)
}

// AddCondition adds a condition
func (msg *IRODSMessageQueryRequest) AddCondition(key common.ICATColumnNumber, val string) {
	escapedVal := util.EscapeXMLSpecialChars(val)
	msg.Conditions.Add(int(key), escapedVal)
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageQueryRequest) AddKeyVal(key common.KeyWord, val string) {
	escapedVal := util.EscapeXMLSpecialChars(val)
	msg.KeyVals.Add(string(key), escapedVal)
}

// GetBytes returns byte array
func (msg *IRODSMessageQueryRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQueryRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageQueryRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageQueryRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
