package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageAdminChangeUserTypeRequest stores chage user type request
type IRODSMessageAdminChangeUserTypeRequest IRODSMessageAdminRequest

// NewIRODSMessageAdminChangeUserTypeRequest creates a new IRODSMessageAdminChangeUserTypeRequest
func NewIRODSMessageAdminChangeUserTypeRequest(username string, zone string, userType types.IRODSUserType) *IRODSMessageAdminChangeUserTypeRequest {
	request := &IRODSMessageAdminChangeUserTypeRequest{
		Action: "modify",
		Target: "user",
	}

	request.Arg2 = fmt.Sprintf("%s#%s", username, zone)
	request.Arg3 = "type"
	request.Arg4 = string(userType)
	request.Arg5 = zone

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminChangeUserTypeRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminChangeUserTypeRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminChangeUserTypeRequest) GetMessage() (*IRODSMessage, error) {
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
func (msg *IRODSMessageAdminChangeUserTypeRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
