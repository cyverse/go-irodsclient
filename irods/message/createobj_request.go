package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageCreateobjRequest stores data object creation request
type IRODSMessageCreateobjRequest IRODSMessageDataObjectRequest

// NewIRODSMessageCreateobjRequest creates a IRODSMessageCreateobjRequest message
func NewIRODSMessageCreateobjRequest(path string, resource string, mode types.FileOpenMode, force bool) *IRODSMessageCreateobjRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageCreateobjRequest{
		Path:          path,
		CreateMode:    0644,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.KeyVals.Add(string(common.DATA_TYPE_KW), string(types.GENERIC_DT))

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	return request
}

// NewIRODSMessageCreateobjRequestWithKeyVals creates a IRODSMessageCreateobjRequest message with given keyvals
func NewIRODSMessageCreateobjRequestWithKeyVals(path string, resource string, mode types.FileOpenMode, force bool, keyvals map[string]string) *IRODSMessageCreateobjRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageCreateobjRequest{
		Path:          path,
		CreateMode:    0644,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	// if data type is not set
	if _, ok := keyvals[string(common.DATA_TYPE_KW)]; !ok {
		request.KeyVals.Add(string(common.DATA_TYPE_KW), string(types.GENERIC_DT))
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	for key, val := range keyvals {
		request.KeyVals.Add(key, val)
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageCreateobjRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageCreateobjRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCreateobjRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageCreateobjRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_CREATE_AN),
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
