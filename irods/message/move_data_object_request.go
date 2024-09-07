package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageMoveDataObjectRequest stores data object move request
type IRODSMessageMoveDataObjectRequest struct {
	XMLName xml.Name `xml:"DataObjCopyInp_PI"`
	Paths   []IRODSMessageDataObjectRequest
}

// NewIRODSMessageMoveDataObjectRequest creates a IRODSMessageMoveDataObjectRequest message
func NewIRODSMessageMoveDataObjectRequest(srcPath string, destPath string) *IRODSMessageMoveDataObjectRequest {
	return &IRODSMessageMoveDataObjectRequest{
		Paths: []IRODSMessageDataObjectRequest{
			{
				Path:          srcPath,
				CreateMode:    0,
				OpenFlags:     0,
				Offset:        0,
				Size:          0,
				Threads:       0,
				OperationType: int(common.OPER_TYPE_RENAME_DATA_OBJ),
				KeyVals: IRODSMessageSSKeyVal{
					Length: 0,
				},
			},
			{
				Path:          destPath,
				CreateMode:    0,
				OpenFlags:     0,
				Offset:        0,
				Size:          0,
				Threads:       0,
				OperationType: int(common.OPER_TYPE_RENAME_DATA_OBJ),
				KeyVals: IRODSMessageSSKeyVal{
					Length: 0,
				},
			},
		},
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageMoveDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageMoveDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageMoveDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_RENAME_AN),
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
func (msg *IRODSMessageMoveDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
