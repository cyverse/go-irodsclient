package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageMkcolRequest stores collection creation request
type IRODSMessageMkcolRequest struct {
	XMLName       xml.Name             `xml:"CollInpNew_PI"`
	Name          string               `xml:"collName"`
	Flags         int                  `xml:"flags"`   // unused
	OperationType int                  `xml:"oprType"` // unused
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageMkcolRequest creates a IRODSMessageMkcolRequest message
func NewIRODSMessageMkcolRequest(name string, recurse bool) *IRODSMessageMkcolRequest {
	request := &IRODSMessageMkcolRequest{
		Name:          name,
		Flags:         0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if recurse {
		request.KeyVals.Add(string(common.RECURSIVE_OPR_KW), "")
	}
	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageMkcolRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
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
