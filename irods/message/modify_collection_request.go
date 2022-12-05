package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageModifyCollectionRequest stores mod coll request
type IRODSMessageModifyCollectionRequest IRODSMessageMakeCollectionRequest

// NewIRODSMessageModifyCollectionRequest creates a IRODSMessageModifyCollectionRequest message
func NewIRODSMessageModifyCollectionRequest(name string) *IRODSMessageModifyCollectionRequest {
	request := &IRODSMessageModifyCollectionRequest{
		Name:          name,
		Flags:         0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageModifyCollectionRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageModifyCollectionRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageModifyCollectionRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageModifyCollectionRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.MOD_COLL_AN),
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
