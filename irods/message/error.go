package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
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
		return nil, fmt.Errorf("invalid count %d, error length is %d", msg.Count, len(msg.Errors))
	}

	if msg.Count == 0 {
		return nil, nil
	}

	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageError) FromBytes(bytes []byte) error {
	if bytes == nil {
		msg.Count = 0
		msg.Errors = nil

		return nil
	}

	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageError) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageError) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
