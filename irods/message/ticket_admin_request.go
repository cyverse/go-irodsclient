package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageTicketAdminRequest stores ticket admin request
type IRODSMessageTicketAdminRequest struct {
	XMLName xml.Name `xml:"ticketAdminInp_PI"`
	Action  string   `xml:"arg1"` // session, create
	Ticket  string   `xml:"arg2"` // ticket number
	Arg3    string   `xml:"arg3"`
	Arg4    string   `xml:"arg4"`
	Arg5    string   `xml:"arg5"`
	Arg6    string   `xml:"arg6"`
}

// NewIRODSMessageTicketAdminRequest creates a new IRODSMessageTicketAdminRequest
func NewIRODSMessageTicketAdminRequest(action string, ticket string, args ...string) *IRODSMessageTicketAdminRequest {
	request := &IRODSMessageTicketAdminRequest{
		Action: action,
		Ticket: ticket,
	}

	if len(args) > 0 {
		request.Arg3 = args[0]
	}

	if len(args) > 1 {
		request.Arg4 = args[1]
	}

	if len(args) > 2 {
		request.Arg5 = args[2]
	}

	if len(args) > 3 {
		request.Arg6 = args[3]
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageTicketAdminRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTicketAdminRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageTicketAdminRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.TICKET_ADMIN_AN),
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
