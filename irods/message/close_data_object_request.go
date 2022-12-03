package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageCloseDataObjectRequest stores data object close request
type IRODSMessageCloseDataObjectRequest IRODSMessageOpenedDataObjectRequest

// NewIRODSMessageCloseDataObjectRequest creates a IRODSMessageCloseDataObjectRequest message
func NewIRODSMessageCloseDataObjectRequest(desc int) *IRODSMessageCloseDataObjectRequest {
	request := &IRODSMessageCloseDataObjectRequest{
		FileDescriptor: desc,
		Size:           0,
		Whence:         0,
		OperationType:  0,
		Offset:         0,
		BytesWritten:   0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageCloseDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageCloseDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCloseDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageCloseDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_CLOSE_AN),
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
