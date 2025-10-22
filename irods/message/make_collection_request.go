package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageMakeCollectionRequest stores collection creation request
type IRODSMessageMakeCollectionRequest struct {
	XMLName       xml.Name             `xml:"CollInpNew_PI"`
	Name          string               `xml:"collName"`
	Flags         int                  `xml:"flags"`   // unused
	OperationType int                  `xml:"oprType"` // unused
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageMakeCollectionRequest creates a IRODSMessageMakeCollectionRequest message
func NewIRODSMessageMakeCollectionRequest(name string, recurse bool) *IRODSMessageMakeCollectionRequest {
	request := &IRODSMessageMakeCollectionRequest{
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
func (msg *IRODSMessageMakeCollectionRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageMakeCollectionRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageMakeCollectionRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageMakeCollectionRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
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
		return nil, errors.Wrapf(err, "failed to build header from irods message")
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageMakeCollectionRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
