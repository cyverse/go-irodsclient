package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageAdminRequestIRODSMessageAdminChangePasswordRequest stores change password request
type IRODSMessageAdminChangePasswordRequest struct {
	XMLName xml.Name `xml:"generalAdminInp_PI"`
	Action  string   `xml:"arg0"` // add, modify, rm, ...
	Target  string   `xml:"arg1"` // user, group, zone, resource, ...
	Arg2    string   `xml:"arg2"`
	Arg3    string   `xml:"arg3"`
	Arg4    string   `xml:"arg4"`
	Arg5    string   `xml:"arg5"`
	Arg6    string   `xml:"arg6"`
	Arg7    string   `xml:"arg7"`
	Arg8    string   `xml:"arg8"` // unused
	Arg9    string   `xml:"arg9"` // unused
}

// NewIRODSMessageAdminChangePasswordRequest creates a new IRODSMessageAdminChangePasswordRequest
func NewIRODSMessageAdminChangePasswordRequest(username string, password string, zone string) *IRODSMessageAdminChangePasswordRequest {
	request := &IRODSMessageAdminChangePasswordRequest{
		Action: "modify",
		Target: "user",
	}

	request.Arg2 = username // zone name
	request.Arg3 = "password"
	request.Arg4 = password // password
	request.Arg5 = zone

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminChangePasswordRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminChangePasswordRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAdminChangePasswordRequest) GetMessage() (*IRODSMessage, error) {
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
func (msg *IRODSMessageAdminChangePasswordRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForPasswordRequest()
}
