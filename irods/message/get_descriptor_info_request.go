package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageGetDescriptorInfoRequest stores data object descriptor info. request
type IRODSMessageGetDescriptorInfoRequest struct {
	FileDescriptor int `json:"fd"`
}

// NewIRODSMessageGetDescriptorInfoRequest creates a IRODSMessageDescriptorInfoRequest message
func NewIRODSMessageGetDescriptorInfoRequest(desc int) *IRODSMessageGetDescriptorInfoRequest {
	return &IRODSMessageGetDescriptorInfoRequest{
		FileDescriptor: desc,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageGetDescriptorInfoRequest) GetBytes() ([]byte, error) {
	jsonBody, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to json")
	}

	jsonBodyBin := base64.StdEncoding.EncodeToString(jsonBody)

	binBytesBuf := IRODSMessageBinBytesBuf{
		Length: len(jsonBody), // use original data's length
		Data:   jsonBodyBin,
	}

	xmlBytes, err := xml.Marshal(binBytesBuf)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDescriptorInfoRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal irods message to xml")
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return errors.Wrapf(err, "failed to decode base64 data")
	}

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal json to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageGetDescriptorInfoRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
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
		return nil, errors.Wrapf(err, "failed to build header from irods message")
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetDescriptorInfoRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
