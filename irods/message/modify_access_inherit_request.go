package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageModifyAccessRequest stores alter access control request
type IRODSMessageModifyAccessInheritRequest IRODSMessageModifyAccessRequest

// NewIRODSMessageModifyAccessInheritRequest creates a IRODSMessageModifyAccessInheritRequest message for altering the access control list of a object or collection.
func NewIRODSMessageModifyAccessInheritRequest(inherit bool, path string, recurse bool, asAdmin bool) *IRODSMessageModifyAccessInheritRequest {
	inheritString := "inherit"
	if !inherit {
		inheritString = "noinherit"
	}

	if asAdmin {
		inheritString = fmt.Sprintf("admin:%s", inheritString)
	}

	recursiveFlag := 0
	if recurse {
		recursiveFlag = 1
	}

	request := &IRODSMessageModifyAccessInheritRequest{
		RecursiveFlag: recursiveFlag,
		AccessLevel:   inheritString,
		UserName:      "",
		Zone:          "",
		Path:          path,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageModifyAccessInheritRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageModifyAccessInheritRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageModifyAccessInheritRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageModifyAccessInheritRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
