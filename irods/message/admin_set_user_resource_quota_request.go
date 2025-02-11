package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageAdminSetUserResourceQuotaRequest stores set user resource quota request
type IRODSMessageAdminSetUserResourceQuotaRequest IRODSMessageAdminRequest

// NewIRODSMessageAdminSetUserResourceQuotaRequest creates a new IRODSMessageAdminSetUserResourceQuotaRequest
func NewIRODSMessageAdminSetUserResourceQuotaRequest(username string, zoneName string, resource string, value string) *IRODSMessageAdminSetUserResourceQuotaRequest {
	request := &IRODSMessageAdminSetUserResourceQuotaRequest{
		Action: "set-quota",
		Target: "user",
	}

	request.Arg2 = fmt.Sprintf("%s#%s", username, zoneName)
	request.Arg3 = resource
	request.Arg4 = value

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminSetUserResourceQuotaRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminSetUserResourceQuotaRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminSetUserResourceQuotaRequest) GetMessage() (*IRODSMessage, error) {
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
func (msg *IRODSMessageAdminSetUserResourceQuotaRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
