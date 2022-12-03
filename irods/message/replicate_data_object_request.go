package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageReplicateDataObjectRequest stores data object replication request
type IRODSMessageReplicateDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageReplicateDataObjectRequest creates a IRODSMessageReplicateDataObjectRequest message
func NewIRODSMessageReplicateDataObjectRequest(path string, resource string) *IRODSMessageReplicateDataObjectRequest {
	request := &IRODSMessageReplicateDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     0,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: int(common.OPER_TYPE_REPLICATE_DATA_OBJ),
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
func (msg *IRODSMessageReplicateDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageReplicateDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageReplicateDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageReplicateDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_REPL_AN),
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
