package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageOpenDataObjectRequest stores data object open request
type IRODSMessageOpenDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageOpenDataObjectRequest creates a IRODSMessageOpenDataObjectRequest message
func NewIRODSMessageOpenDataObjectRequest(path string, resource string, mode types.FileOpenMode) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
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

// NewIRODSMessageOpenobjRequestWithOperation ...
func NewIRODSMessageOpenobjRequestWithOperation(path string, resource string, mode types.FileOpenMode, oper common.OperationType) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: int(oper),
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	return request
}

// NewIRODSMessageOpenobjRequestWithReplicaToken creates a IRODSMessageOpenobjRequest message
func NewIRODSMessageOpenobjRequestWithReplicaToken(path string, mode types.FileOpenMode, resourceHierarchy string, replicaToken string) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.AddKeyVal(common.RESC_HIER_STR_KW, resourceHierarchy)
	request.AddKeyVal(common.REPLICA_TOKEN_KW, replicaToken)

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageOpenDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageOpenDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageOpenDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageOpenDataObjectRequest) GetMessage() (*IRODSMessage, error) {
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
