package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageTicketAdminRequest stores ticket admin request
type IRODSMessageTicketAdminRequest struct {
	XMLName xml.Name             `xml:"ticketAdminInp_PI"`
	Action  string               `xml:"arg1"` // session, create, or mod
	Ticket  string               `xml:"arg2"` // ticket name
	Arg3    string               `xml:"arg3"` // ticket type
	Arg4    string               `xml:"arg4"` // path
	Arg5    string               `xml:"arg5"` // ticket name again
	Arg6    string               `xml:"arg6"`
	KeyVals IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageTicketAdminRequest creates a new IRODSMessageTicketAdminRequest
func NewIRODSMessageTicketAdminRequest(action string, ticket string, args ...string) *IRODSMessageTicketAdminRequest {
	request := &IRODSMessageTicketAdminRequest{
		Action: action,
		Ticket: ticket,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
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

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageTicketAdminRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageTicketAdminRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTicketAdminRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageTicketAdminRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

func (msg *IRODSMessageTicketAdminRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
