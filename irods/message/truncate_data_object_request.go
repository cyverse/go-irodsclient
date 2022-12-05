package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageTruncateDataObjectRequest stores data object truncation request
type IRODSMessageTruncateDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageTruncateDataObjectRequest creates a IRODSMessageTruncateDataObjectRequest message
func NewIRODSMessageTruncateDataObjectRequest(path string, size int64) *IRODSMessageTruncateDataObjectRequest {
	request := &IRODSMessageTruncateDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     0,
		Offset:        0,
		Size:          size,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageTruncateDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageTruncateDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTruncateDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageTruncateDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_TRUNCATE_AN),
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
