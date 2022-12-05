package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageCopyDataObjectRequest stores data object copy request
type IRODSMessageCopyDataObjectRequest struct {
	XMLName xml.Name `xml:"DataObjCopyInp_PI"`
	Paths   []IRODSMessageDataObjectRequest
}

// NewIRODSMessageCopyDataObjectRequest creates a IRODSMessageCopyDataObjectRequest message
func NewIRODSMessageCopyDataObjectRequest(srcPath string, destPath string) *IRODSMessageCopyDataObjectRequest {
	return &IRODSMessageCopyDataObjectRequest{
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
}

// GetBytes returns byte array
func (msg *IRODSMessageCopyDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCopyDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageCopyDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
