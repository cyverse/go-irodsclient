package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

type IRODSMessageError struct {
	XMLName xml.Name `xml:"RError_PI"`
	Count   int      `xml:"count"`
	Errors  []ErrorMsg
}

type ErrorMsg struct {
	XMLName xml.Name `xml:"RErrMsg_PI"`
	Status  int      `xml:"status"`
	Message string   `xml:"msg"`
}

func NewIRODSMessageError(status int, msg string) *IRODSMessageError {
	return &IRODSMessageError{
		Count: 1,
		Errors: []ErrorMsg{{
			Status:  status,
			Message: msg,
		}},
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageError) GetBytes() ([]byte, error) {
	if msg.Count != len(msg.Errors) {
		return nil, xerrors.Errorf("invalid count %d, error length is %d", msg.Count, len(msg.Errors))
	}

	if msg.Count == 0 {
		return nil, nil
	}

	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageError) FromBytes(bytes []byte) error {
	if bytes == nil {
		msg.Count = 0
		msg.Errors = nil

		return nil
	}

	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageError) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REPLY_TYPE,
		Message: nil,
		Error:   bytes,
		Bs:      nil,
		IntInfo: int32(common.ACTION_FAILED_ERR),
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageError) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return xerrors.Errorf("failed to get irods message from message body: %w", err)
	}
	return nil
}
