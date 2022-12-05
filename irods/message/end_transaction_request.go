package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageEndTransactionRequest stores collection creation request
type IRODSMessageEndTransactionRequest struct {
	XMLName  xml.Name `xml:"endTransactionInp_PI"`
	Action   string   `xml:"arg0"`
	Argument string   `xml:"arg1"` // unused
}

// NewIRODSMessageEndTransactionRequest creates a IRODSMessageEndTransactionRequest message
func NewIRODSMessageEndTransactionRequest(commit bool) *IRODSMessageEndTransactionRequest {
	var action string

	if commit {
		action = "commit"
	} else {
		action = "rollback"
	}

	return &IRODSMessageEndTransactionRequest{
		Action: action,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageEndTransactionRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageEndTransactionRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageEndTransactionRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.END_TRANSACTION_AN),
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
