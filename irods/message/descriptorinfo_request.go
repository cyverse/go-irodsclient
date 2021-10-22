package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageDescriptorInfoRequest stores data object descriptor info. request
type IRODSMessageDescriptorInfoRequest struct {
	FileDescriptor int `json:"fd"`
}

// NewIRODSMessageDescriptorInfoRequest creates a IRODSMessageDescriptorInfoRequest message
func NewIRODSMessageDescriptorInfoRequest(desc int) *IRODSMessageDescriptorInfoRequest {
	return &IRODSMessageDescriptorInfoRequest{
		FileDescriptor: desc,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageDescriptorInfoRequest) GetBytes() ([]byte, error) {
	jsonBody, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	jsonBodyBin := base64.StdEncoding.EncodeToString(jsonBody)

	binBytesBuf := IRODSMessageBinBytesBuf{
		Length: len(jsonBody), // use original data's length
		Data:   jsonBodyBin,
	}

	xmlBytes, err := xml.Marshal(binBytesBuf)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageDescriptorInfoRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return err
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBody, msg)
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
