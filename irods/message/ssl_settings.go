package message

import (
	"encoding/xml"

	"golang.org/x/xerrors"
)

// IRODSMessageSSLSettings stores ssl settings
type IRODSMessageSSLSettings struct {
	EncryptionAlgorithm string
	EncryptionKeySize   uint32
	SaltSize            uint32
	HashRounds          uint32
}

// NewIRODSMessageSSLSettings creates a IRODSMessageSSLSettings message
func NewIRODSMessageSSLSettings(algorithm string, keySize int, saltSize int, hashRounds int) *IRODSMessageSSLSettings {
	return &IRODSMessageSSLSettings{
		EncryptionAlgorithm: algorithm,
		EncryptionKeySize:   uint32(keySize),
		SaltSize:            uint32(saltSize),
		HashRounds:          uint32(hashRounds),
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageSSLSettings) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSSLSettings) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageSSLSettings) GetMessage() (*IRODSMessage, error) {
	msgHeader := IRODSMessageHeader{
		Type:       MessageType(msg.EncryptionAlgorithm),
		MessageLen: msg.EncryptionKeySize,
		ErrorLen:   msg.SaltSize,
		BsLen:      msg.HashRounds,
		IntInfo:    0,
	}

	return &IRODSMessage{
		Header: &msgHeader,
		Body:   nil,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageSSLSettings) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.EncryptionAlgorithm = string(msgIn.Header.Type)
	msg.EncryptionKeySize = msgIn.Header.MessageLen
	msg.SaltSize = msgIn.Header.ErrorLen
	msg.HashRounds = msgIn.Header.BsLen

	return nil
}

func (msg *IRODSMessageSSLSettings) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
