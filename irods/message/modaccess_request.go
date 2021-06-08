package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageModAccessRequest stores alter access control request
type IRODSMessageModAccessRequest struct {
	XMLName       xml.Name `xml:"modAccessControlInp_PI"`
	RecursiveFlag int      `xml:"recursiveFlag"`
	AccessLevel   string   `xml:"accessLevel"`
	UserName      string   `xml:"userName"`
	Zone          string   `xml:"zone"`
	Path          string   `xml:"path"`
}

// NewIRODSMessageModAccessRequest creates a IRODSMessageModAccessRequest message for altering the access control list of a object or collection.
func NewIRODSMessageModAccessRequest(accessLevel, user, zone, path string, recursive, asAdmin bool) *IRODSMessageModAccessRequest {
	if asAdmin {
		accessLevel = fmt.Sprintf("admin:%s", accessLevel)
	}

	request := &IRODSMessageModAccessRequest{
		RecursiveFlag: 0,
		AccessLevel:   accessLevel,
		UserName:      user,
		Zone:          zone,
		Path:          path,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageModAccessRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageModAccessRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageModAccessRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.MOD_ACCESS_CONTROL_AN),
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
