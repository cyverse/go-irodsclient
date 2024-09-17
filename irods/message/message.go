package message

import (
	"bytes"
	"encoding/xml"

	"golang.org/x/xerrors"
)

// MessageType is a message type
type MessageType string

// IRODSMessageHeader defines a message header
type IRODSMessageHeader struct {
	XMLName    xml.Name    `xml:"MsgHeader_PI"`
	Type       MessageType `xml:"type"`
	MessageLen uint32      `xml:"msgLen"`
	ErrorLen   uint32      `xml:"errorLen"`
	BsLen      uint32      `xml:"bsLen"`
	IntInfo    int32       `xml:"intInfo"`
}

// IRODSMessageBody defines a message body
type IRODSMessageBody struct {
	Type    MessageType
	Message []byte
	Error   []byte
	Bs      []byte
	IntInfo int32
}

// IRODSMessage defines a message
type IRODSMessage struct {
	Header *IRODSMessageHeader
	Body   *IRODSMessageBody
}

// IRODSMessageSerializationInterface is an interface for serializaing/deserializing of message
type IRODSMessageSerializationInterface interface {
	GetBytes() ([]byte, error)
	FromBytes(bodyBytes []byte, bsBytes []byte) error
}

// MakeIRODSMessageHeader makes a message header
func MakeIRODSMessageHeader(messageType MessageType, messageLen uint32, errorLen uint32, bsLen uint32, intInfo int32) *IRODSMessageHeader {
	return &IRODSMessageHeader{
		Type:       messageType,
		MessageLen: messageLen,
		ErrorLen:   errorLen,
		BsLen:      bsLen,
		IntInfo:    intInfo,
	}
}

// GetBytes returns byte array
func (header *IRODSMessageHeader) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(header)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (header *IRODSMessageHeader) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, header)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetBytes returns byte array
func (body *IRODSMessageBody) GetBytes() ([]byte, error) {
	messageBuffer := &bytes.Buffer{}
	if body.Message != nil {
		_, err := messageBuffer.Write(body.Message)
		if err != nil {
			return nil, xerrors.Errorf("failed to write to message buffer: %w", err)
		}
	}

	if body.Error != nil {
		_, err := messageBuffer.Write(body.Error)
		if err != nil {
			return nil, xerrors.Errorf("failed to write to message buffer: %w", err)
		}
	}

	if body.Bs != nil {
		_, err := messageBuffer.Write(body.Bs)
		if err != nil {
			return nil, xerrors.Errorf("failed to write to message buffer: %w", err)
		}
	}

	return messageBuffer.Bytes(), nil
}

// GetBytesWithoutBS returns byte array of body without BS
func (body *IRODSMessageBody) GetBytesWithoutBS() ([]byte, error) {
	messageBuffer := new(bytes.Buffer)
	if body.Message != nil {
		_, err := messageBuffer.Write(body.Message)
		if err != nil {
			return nil, xerrors.Errorf("failed to write to message buffer: %w", err)
		}
	}

	if body.Error != nil {
		_, err := messageBuffer.Write(body.Error)
		if err != nil {
			return nil, xerrors.Errorf("failed to write to message buffer: %w", err)
		}
	}

	return messageBuffer.Bytes(), nil
}

// FromBytes returns struct from bytes
func (body *IRODSMessageBody) FromBytes(header *IRODSMessageHeader, bodyBytes []byte, bsBytes []byte) error {
	if len(bodyBytes) < (int(header.MessageLen) + int(header.ErrorLen)) {
		return xerrors.Errorf("bodyBytes given is too short to be parsed")
	}

	if len(bsBytes) < int(header.BsLen) {
		return xerrors.Errorf("bsBytes given is too short to be parsed")
	}

	offset := 0
	body.Message = bodyBytes[offset : offset+int(header.MessageLen)]

	offset += int(header.MessageLen)
	body.Error = bodyBytes[offset : offset+int(header.ErrorLen)]

	body.Bs = bsBytes[:int(header.BsLen)]

	return nil
}

// BuildHeader returns IRODSMessageHeader
func (body *IRODSMessageBody) BuildHeader() (*IRODSMessageHeader, error) {
	messageLen := 0
	errorLen := 0
	bsLen := 0

	if body.Message != nil {
		messageLen = len(body.Message)
	}

	if body.Error != nil {
		errorLen = len(body.Error)
	}

	if body.Bs != nil {
		bsLen = len(body.Bs)
	}

	h := MakeIRODSMessageHeader(body.Type, uint32(messageLen), uint32(errorLen), uint32(bsLen), body.IntInfo)
	return h, nil
}
