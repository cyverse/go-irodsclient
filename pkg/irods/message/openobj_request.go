package message

import (
	"encoding/xml"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

// IRODSMessageOpenobjRequest stores data object open request
type IRODSMessageOpenobjRequest IRODSMessageDataObjectRequest

// NewIRODSMessageOpenobjRequest creates a IRODSMessageOpenobjRequest message
func NewIRODSMessageOpenobjRequest(path string, resource string, mode types.FileOpenMode) *IRODSMessageOpenobjRequest {
	flags, _ := types.GetFileOpenFlagSeekToEnd(mode)

	request := &IRODSMessageOpenobjRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flags,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
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
func (msg *IRODSMessageOpenobjRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageOpenobjRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageOpenobjRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageOpenobjRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_OPEN_AN),
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
