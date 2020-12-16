package message

import (
	"encoding/xml"
)

// negotiation constants
const (
	// Token sent to the server to request negotiation
	REQUEST_NEGOTIATION string = "request_server_negotiation"

	// Keywords
	CS_NEG_SID_KW    string = "cs_neg_sid_kw"
	CS_NEG_RESULT_KW string = "cs_neg_result_kw"
)

// IRODSMessageCSNegotiation stores client-server negotiation message
type IRODSMessageCSNegotiation struct {
	XMLName xml.Name `xml:"CS_NEG_PI"`
	Status  int      `xml:"status"`
	Result  string   `xml:"result"`
}

// NewIRODSMessageCSNegotiation creates a IRODSMessageCSNegotiation message
func NewIRODSMessageCSNegotiation(status int, result string) *IRODSMessageCSNegotiation {
	negotiation := &IRODSMessageCSNegotiation{
		Status: 0,
		Result: result,
	}

	return negotiation
}

// ToXML returns XML byte array
func (negotiation *IRODSMessageCSNegotiation) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(negotiation)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (negotiation *IRODSMessageCSNegotiation) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, negotiation)
	return err
}
