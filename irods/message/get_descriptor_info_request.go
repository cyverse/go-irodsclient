package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
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
		return nil, xerrors.Errorf("failed to marshal irods message to json: %w", err)
	}

	jsonBodyBin := base64.StdEncoding.EncodeToString(jsonBody)

	binBytesBuf := IRODSMessageBinBytesBuf{
		Length: len(jsonBody), // use original data's length
		Data:   jsonBodyBin,
	}

	xmlBytes, err := xml.Marshal(binBytesBuf)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDescriptorInfoRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return xerrors.Errorf("failed to decode base64 data: %w", err)
	}

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal json to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageGetDescriptorInfoRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
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
