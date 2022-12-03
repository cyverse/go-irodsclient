package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageGetDataObjectStatRequest stores file stat request
type IRODSMessageGetDataObjectStatRequest IRODSMessageDataObjectRequest

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectStatRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDataObjectStatRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageGetDataObjectStatRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.OBJ_STAT_AN),
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
