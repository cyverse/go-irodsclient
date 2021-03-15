package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageFileStatRequest stores file stat request
type IRODSMessageFileStatRequest struct {
	XMLName           xml.Name         `xml:"fileStatInp_PI"`
	Host              IRODSMessageHost `xml:"RHostAddr_PI"`
	Path              string           `xml:"fileName"`
	ResourceHierarchy string           `xml:"rescHier"`
	ObjectPath        string           `xml:"objPath"`
	ResourceID        int64            `xml:"rescId"`
}

// NewIRODSMessageFileStatRequest creates a IRODSMessageFileStatRequest message
func NewIRODSMessageFileStatRequest(resource *types.IRODSResource, obj *types.IRODSDataObject, replica *types.IRODSReplica) (*IRODSMessageFileStatRequest, error) {
	host, err := NewIRODSMessageHost(resource)
	if err != nil {
		return nil, err
	}

	if resource.Name != replica.ResourceName {
		return nil, fmt.Errorf("Resource name %s does not match replica resource name %s", resource.Name, replica.ResourceName)
	}

	request := &IRODSMessageFileStatRequest{
		Host:              *host,
		Path:              replica.Path,
		ResourceHierarchy: replica.ResourceHierarchy,
		ObjectPath:        obj.Path,
		ResourceID:        resource.RescID,
	}

	return request, nil
}

// GetBytes returns byte array
func (msg *IRODSMessageFileStatRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageFileStatRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageFileStatRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.FILE_STAT_AN),
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
