package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
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
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetProcessstatRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageGetProcessstatRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetProcessstatRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
