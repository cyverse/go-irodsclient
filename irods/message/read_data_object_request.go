package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageReadDataObjectRequest stores data object read request
type IRODSMessageReadDataObjectRequest IRODSMessageOpenedDataObjectRequest

// NewIRODSMessageReadDataObjectRequest creates a IRODSMessageReadDataObjectRequest message
func NewIRODSMessageReadDataObjectRequest(desc int, len int) *IRODSMessageReadDataObjectRequest {
	request := &IRODSMessageReadDataObjectRequest{
		FileDescriptor: desc,
		Size:           int64(len),
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
func (msg *IRODSMessageReadDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageReadDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageReadDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageReadDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_READ_AN),
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
func (msg *IRODSMessageReadDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
