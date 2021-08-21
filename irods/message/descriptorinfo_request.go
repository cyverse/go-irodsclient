package message

import (
	"encoding/json"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageDescriptorInfoRequest stores data object descriptor info. request
// Uses JSON, not XML
type IRODSMessageDescriptorInfoRequest struct {
	FileDescriptor int `json:"fd"`
}

// NewIRODSMessageDescriptorInfoRequest creates a IRODSMessageDescriptorInfoRequest message
func NewIRODSMessageDescriptorInfoRequest(desc int) *IRODSMessageDescriptorInfoRequest {
	request := &IRODSMessageDescriptorInfoRequest{
		FileDescriptor: desc,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageDescriptorInfoRequest) GetBytes() ([]byte, error) {
	jsonBytes, err := json.Marshal(msg)
	return jsonBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageDescriptorInfoRequest) FromBytes(bytes []byte) error {
	err := json.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageDescriptorInfoRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.GET_FILE_DESCRIPTOR_INFO_APN),
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
