package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageSeekDataObjectRequest stores data object seek request
type IRODSMessageSeekDataObjectRequest IRODSMessageOpenedDataObjectRequest

// NewIRODSMessageSeekDataObjectRequest creates a IRODSMessageSeekDataObjectRequest message
func NewIRODSMessageSeekDataObjectRequest(desc int, offset int64, whence types.Whence) *IRODSMessageSeekDataObjectRequest {
	request := &IRODSMessageSeekDataObjectRequest{
		FileDescriptor: desc,
		Size:           0,
		Whence:         int(whence),
		OperationType:  0,
		Offset:         offset,
		BytesWritten:   0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageSeekDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageSeekDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSeekDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageSeekDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_LSEEK_AN),
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
func (msg *IRODSMessageSeekDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
