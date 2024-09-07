package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageCopyDataObjectRequest stores data object copy request
type IRODSMessageCopyDataObjectRequest struct {
	XMLName xml.Name `xml:"DataObjCopyInp_PI"`
	Paths   []IRODSMessageDataObjectRequest
}

// NewIRODSMessageCopyDataObjectRequest creates a IRODSMessageCopyDataObjectRequest message
func NewIRODSMessageCopyDataObjectRequest(srcPath string, destPath string, force bool) *IRODSMessageCopyDataObjectRequest {
	request := &IRODSMessageCopyDataObjectRequest{
		Paths: []IRODSMessageDataObjectRequest{
			{
				Path:          srcPath,
				CreateMode:    0,
				OpenFlags:     0,
				Offset:        0,
				Size:          0,
				Threads:       0,
				OperationType: int(common.OPER_TYPE_COPY_DATA_OBJ_SRC),
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
				OperationType: int(common.OPER_TYPE_COPY_DATA_OBJ_DEST),
				KeyVals: IRODSMessageSSKeyVal{
					Length: 0,
				},
			},
		},
	}

	if force {
		request.Paths[1].KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageCopyDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	if len(msg.Paths) >= 2 {
		msg.Paths[1].KeyVals.Add(string(key), val)
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageCopyDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCopyDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageCopyDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_COPY_AN),
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
func (msg *IRODSMessageCopyDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
