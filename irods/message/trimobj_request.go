package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageTrimobjRequest stores data object replication request
type IRODSMessageTrimobjRequest IRODSMessageDataObjectRequest

// NewIRODSMessageTrimobjRequest creates a IRODSMessageReplobjRequest message
func NewIRODSMessageTrimobjRequest(path string, resource string, minCopies int, minAgeMinutes int) *IRODSMessageTrimobjRequest {
	request := &IRODSMessageTrimobjRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     0,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.RESC_NAME_KW), resource)
	}

	if minCopies > 0 {
		request.KeyVals.Add(string(common.COPIES_KW), fmt.Sprintf("%d", minCopies))
	}

	if minAgeMinutes > 0 {
		request.KeyVals.Add(string(common.AGE_KW), fmt.Sprintf("%d", minAgeMinutes))
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageTrimobjRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageTrimobjRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTrimobjRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageTrimobjRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_TRIM_AN),
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
