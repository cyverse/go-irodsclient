package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// negotiation constants
const (
	RODS_MESSAGE_CS_NEG_TYPE MessageType = "RODS_CS_NEG_T"

	// Keywords
	CS_NEG_SID_KW string = "cs_neg_sid_kw" // unknown
	negResultKW   string = "cs_neg_result_kw"
)

// IRODSMessageCSNegotiation stores client-server negotiation message
type IRODSMessageCSNegotiation struct {
	XMLName xml.Name `xml:"CS_NEG_PI"`
	Status  int      `xml:"status"`
	Result  string   `xml:"result"`
}

// NewIRODSMessageCSNegotiation creates a IRODSMessageCSNegotiation message
func NewIRODSMessageCSNegotiation(status int, result types.CSNegotiationPolicy) *IRODSMessageCSNegotiation {
	negotiationResultString := fmt.Sprintf("%s=%s;", negResultKW, string(result))
	return &IRODSMessageCSNegotiation{
		Status: status,
		Result: negotiationResultString,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageCSNegotiation) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCSNegotiation) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageCSNegotiation) CheckError() error {
	if msg.Status < 0 {
		return types.NewIRODSErrorWithString(common.ErrorCode(msg.Status), msg.Result)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageCSNegotiation) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_CS_NEG_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageCSNegotiation) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
