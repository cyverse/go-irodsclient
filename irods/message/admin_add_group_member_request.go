package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAdminAddGroupMemberRequest stores add group member request
type IRODSMessageAdminAddGroupMemberRequest IRODSMessageAdminRequest

// NewIRODSMessageAdminAddGroupMemberRequest creates a new IRODSMessageAdminAddGroupMemberRequest
func NewIRODSMessageAdminAddGroupMemberRequest(groupName string, username string, zoneName string) *IRODSMessageAdminAddGroupMemberRequest {
	request := &IRODSMessageAdminAddGroupMemberRequest{
		Action: "modify",
		Target: "group",
	}

	request.Arg2 = groupName
	request.Arg3 = "add"
	request.Arg4 = username
	request.Arg5 = zoneName

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminAddGroupMemberRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminAddGroupMemberRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminAddGroupMemberRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
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
		return nil, errors.Wrapf(err, "failed to build header from irods message")
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAdminAddGroupMemberRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
