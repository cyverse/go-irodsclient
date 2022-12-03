package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// IRODSMessageGetProcessstatRequest stores process stat request
type IRODSMessageGetProcessstatRequest struct {
	XMLName xml.Name             `xml:"ProcStatInp_PI"`
	Address string               `xml:"addr"`
	Zone    string               `xml:"rodsZone"`
	KeyVals IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageGetProcessstatRequest creates a IRODSMessageGetProcessstatRequest message
func NewIRODSMessageGetProcessstatRequest(address string, zone string) *IRODSMessageGetProcessstatRequest {
	return &IRODSMessageGetProcessstatRequest{
		Address: address,
		Zone:    zone,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageGetProcessstatRequest) AddKeyVal(key common.KeyWord, val string) {
	escapedVal := util.EscapeXMLSpecialChars(val)
	msg.KeyVals.Add(string(key), escapedVal)
}

// GetBytes returns byte array
func (msg *IRODSMessageGetProcessstatRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetProcessstatRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageGetProcessstatRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.PROC_STAT_AN),
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
