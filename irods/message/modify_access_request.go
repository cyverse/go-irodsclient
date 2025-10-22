package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageModifyAccessRequest stores alter access control request
type IRODSMessageModifyAccessRequest struct {
	XMLName       xml.Name `xml:"modAccessControlInp_PI"`
	RecursiveFlag int      `xml:"recursiveFlag"`
	AccessLevel   string   `xml:"accessLevel"`
	UserName      string   `xml:"userName"`
	Zone          string   `xml:"zone"`
	Path          string   `xml:"path"`
}

// NewIRODSMessageModifyAccessRequest creates a IRODSMessageModAccessRequest message for altering the access control list of a object or collection.
func NewIRODSMessageModifyAccessRequest(accessLevel string, user string, zone string, path string, recurse bool, asAdmin bool) *IRODSMessageModifyAccessRequest {
	if asAdmin {
		accessLevel = fmt.Sprintf("admin:%s", accessLevel)
	}

	recursiveFlag := 0
	if recurse {
		recursiveFlag = 1
	}

	request := &IRODSMessageModifyAccessRequest{
		RecursiveFlag: recursiveFlag,
		AccessLevel:   accessLevel,
		UserName:      user,
		Zone:          zone,
		Path:          path,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageModifyAccessRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageModifyAccessRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageModifyAccessRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
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
		return nil, errors.Wrapf(err, "failed to build header from irods message")
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageModifyAccessRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
