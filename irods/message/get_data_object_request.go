package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageGetDataObjectRequest stores file get request
type IRODSMessageGetDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageGetDataObjectRequest creates a IRODSMessageGetDataObjectRequest message
func NewIRODSMessageGetDataObjectRequest(path string, resource string, fileLength int64, threads int) *IRODSMessageGetDataObjectRequest {
	request := &IRODSMessageGetDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     0,
		Offset:        0,
		Size:          fileLength,
		Threads:       threads,
		OperationType: int(common.OPER_TYPE_GET_DATA_OBJ),
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageGetDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageGetDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_GET_AN),
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
func (msg *IRODSMessageGetDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
