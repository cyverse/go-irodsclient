package message

import (
	"encoding/xml"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
)

// IRODSMessageMkcolRequest stores collection creation request
type IRODSMessageMkcolRequest struct {
	XMLName       xml.Name             `xml:"CollInpNew_PI"`
	Name          string               `xml:"collName"`
	Flags         int                  `xml:"flags"`
	OperationType int                  `xml:"oprType"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageMkcolRequest creates a IRODSMessageMkcolRequest message
func NewIRODSMessageMkcolRequest(name string, flags int, operationType int) *IRODSMessageMkcolRequest {
	return &IRODSMessageMkcolRequest{
		Name:          name,
		Flags:         flags,
		OperationType: operationType,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageMkcolRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// SetRecurse sets recursion
func (msg *IRODSMessageMkcolRequest) SetRecurse() {
	msg.KeyVals.Add(string(common.RECURSIVE_OPR__KW), "")
}

// GetBytes returns byte array
func (msg *IRODSMessageMkcolRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageMkcolRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageMkcolRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.COLL_CREATE_AN),
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
