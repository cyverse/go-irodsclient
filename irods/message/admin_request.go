package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAdminRequest stores alter metadata request
type IRODSMessageAdminRequest struct {
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

func NewIRODSMessageAdminRequest(action, target string, args ...string) *IRODSMessageAdminRequest {
	request := &IRODSMessageAdminRequest{
		Action: action,
		Target: target,
	}

	if len(args) > 0 {
		request.Arg2 = args[0]
	}

	if len(args) > 1 {
		request.Arg3 = args[1]
	}

	if len(args) > 2 {
		request.Arg4 = args[2]
	}

	if len(args) > 3 {
		request.Arg5 = args[3]
	}

	if len(args) > 4 {
		request.Arg6 = args[4]
	}

	if len(args) > 5 {
		request.Arg7 = args[5]
	}

	if len(args) > 6 {
		request.Arg8 = args[6]
	}

	if len(args) > 7 {
		request.Arg9 = args[7]
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageAdminRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAdminRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageAdminRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
