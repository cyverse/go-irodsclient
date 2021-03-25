package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageUserAdminRequest stores alter metadata request
type IRODSMessageUserAdminRequest struct {
	XMLName xml.Name `xml:"userAdminInp_PI"`
	Action  string   `xml:"arg0"` // mkuser, mkgroup, modify
	Arg1    string   `xml:"arg1"`
	Arg2    string   `xml:"arg2"`
	Arg3    string   `xml:"arg3"`
	Arg4    string   `xml:"arg4"`
	Arg5    string   `xml:"arg5"`
	Arg6    string   `xml:"arg6"`
	Arg7    string   `xml:"arg7"`
	Arg8    string   `xml:"arg8"` // unused
	Arg9    string   `xml:"arg9"` // unused
}

func NewIRODSMessageUserAdminRequest(action string, args ...string) *IRODSMessageUserAdminRequest {
	request := &IRODSMessageUserAdminRequest{
		Action: action,
	}

	if len(args) > 0 {
		request.Arg1 = args[0]
	}

	if len(args) > 1 {
		request.Arg2 = args[1]
	}

	if len(args) > 2 {
		request.Arg3 = args[2]
	}

	if len(args) > 3 {
		request.Arg4 = args[3]
	}

	if len(args) > 4 {
		request.Arg5 = args[4]
	}

	if len(args) > 5 {
		request.Arg6 = args[5]
	}

	if len(args) > 6 {
		request.Arg7 = args[6]
	}

	if len(args) > 7 {
		request.Arg8 = args[7]
	}

	if len(args) > 8 {
		request.Arg9 = args[8]
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageUserAdminRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageUserAdminRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageUserAdminRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.USER_ADMIN_AN),
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
