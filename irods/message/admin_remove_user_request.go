package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageAdminRemoveUserRequest stores remove user request
type IRODSMessageAdminRemoveUserRequest IRODSMessageAdminRequest

// NewIRODSMessageAdminRemoveUserRequest creates a new IRODSMessageAdminRemoveUserRequest
func NewIRODSMessageAdminRemoveUserRequest(username string, zoneName string, userType types.IRODSUserType) *IRODSMessageAdminRemoveUserRequest {
	target := "user"

	//if userType == types.IRODSUserRodsGroup {
	//	target = "group"
	//}

	request := &IRODSMessageAdminRemoveUserRequest{
		Action: "rm",
		Target: target,
	}

	request.Arg2 = username
	request.Arg3 = zoneName

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminRemoveUserRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminRemoveUserRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminRemoveUserRequest) GetMessage() (*IRODSMessage, error) {
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
func (msg *IRODSMessageAdminRemoveUserRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
