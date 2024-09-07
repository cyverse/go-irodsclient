package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageGetFileStatRequest stores file stat request
type IRODSMessageGetFileStatRequest struct {
	XMLName           xml.Name         `xml:"fileStatInp_PI"`
	Host              IRODSMessageHost `xml:"RHostAddr_PI"`
	Path              string           `xml:"fileName"`
	ResourceHierarchy string           `xml:"rescHier"`
	ObjectPath        string           `xml:"objPath"`
	ResourceID        int64            `xml:"rescId"`
}

// NewIRODSMessageGetFileStatRequest creates a IRODSMessageGetFileStatRequest message
func NewIRODSMessageGetFileStatRequest(resource *types.IRODSResource, obj *types.IRODSDataObject, replica *types.IRODSReplica) (*IRODSMessageGetFileStatRequest, error) {
	host, err := NewIRODSMessageHost(resource)
	if err != nil {
		return nil, xerrors.Errorf("failed to create irods host message: %w", err)
	}

	if resource.Name != replica.ResourceName {
		return nil, xerrors.Errorf("resource name %q does not match replica resource name %q", resource.Name, replica.ResourceName)
	}

	request := &IRODSMessageGetFileStatRequest{
		Host:              *host,
		Path:              replica.Path,
		ResourceHierarchy: replica.ResourceHierarchy,
		ObjectPath:        obj.Path,
		ResourceID:        resource.RescID,
	}

	return request, nil
}

// GetBytes returns byte array
func (msg *IRODSMessageGetFileStatRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetFileStatRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageGetFileStatRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetFileStatRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
