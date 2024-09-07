package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageWriteDataObjectRequest stores data object read request
// type IRODSMessageWriteDataObjectRequest IRODSMessageOpenedDataObjectRequest
type IRODSMessageWriteDataObjectRequest struct {
	IRODSMessageOpenedDataObjectRequest
	Data []byte `xml:"-"`
}

// NewIRODSMessageWriteDataObjectRequest creates a IRODSMessageWriteDataObjectRequest message
func NewIRODSMessageWriteDataObjectRequest(desc int, data []byte) *IRODSMessageWriteDataObjectRequest {
	request := &IRODSMessageWriteDataObjectRequest{
		IRODSMessageOpenedDataObjectRequest: IRODSMessageOpenedDataObjectRequest{
			FileDescriptor: desc,
			Size:           int64(len(data)),
			Whence:         0,
			OperationType:  0,
			Offset:         0,
			BytesWritten:   0,
			KeyVals: IRODSMessageSSKeyVal{
				Length: 0,
			},
		},
		Data: data,
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageWriteDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageWriteDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageWriteDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageWriteDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      msg.Data,
		IntInfo: int32(common.DATA_OBJ_WRITE_AN),
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
func (msg *IRODSMessageWriteDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
