package message

import (
	"encoding/xml"
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
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
		return common.MakeIRODSErrorFromString(common.ErrorCode(msg.Status), msg.Result)
	}
	return nil
}

// GetMessageBody builds a message body
func (msg *IRODSMessageCSNegotiation) GetMessageBody() (*IRODSMessageBody, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	return &IRODSMessageBody{
		Type:    RODS_MESSAGE_CS_NEG_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}, nil
}

// FromMessageBody returns struct from IRODSMessageBody
func (msg *IRODSMessageCSNegotiation) FromMessageBody(messageBody *IRODSMessageBody) error {
	err := msg.FromBytes(messageBody.Message)
	return err
}
