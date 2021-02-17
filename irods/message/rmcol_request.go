package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageRmcolRequest stores collection deletion request
type IRODSMessageRmcolRequest struct {
	XMLName       xml.Name             `xml:"CollInpNew_PI"`
	Name          string               `xml:"collName"`
	Flags         int                  `xml:"flags"`
	OperationType int                  `xml:"oprType"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageRmcolRequest creates a IRODSMessageRmcolRequest message
func NewIRODSMessageRmcolRequest(name string, recurse bool, force bool) *IRODSMessageRmcolRequest {
	request := &IRODSMessageRmcolRequest{
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

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageRmcolRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageRmcolRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageRmcolRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageRmcolRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.RM_COLL_AN),
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
