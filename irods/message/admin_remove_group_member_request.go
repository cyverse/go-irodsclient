package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageAdminRemoveGroupMemberRequest stores remove group member request
type IRODSMessageAdminRemoveGroupMemberRequest IRODSMessageAdminRequest

// NewIRODSMessageAdminRemoveGroupMemberRequest creates a new IRODSMessageAdminRemoveGroupMemberRequest
func NewIRODSMessageAdminRemoveGroupMemberRequest(groupName string, username string, zoneName string) *IRODSMessageAdminRemoveGroupMemberRequest {
	request := &IRODSMessageAdminRemoveGroupMemberRequest{
		Action: "modify",
		Target: "group",
	}

	request.Arg2 = groupName
	request.Arg3 = "remove"
	request.Arg4 = username
	request.Arg5 = zoneName

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminRemoveGroupMemberRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminRemoveGroupMemberRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminRemoveGroupMemberRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.GENERAL_ADMIN_AN),
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
func (msg *IRODSMessageAdminRemoveGroupMemberRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
