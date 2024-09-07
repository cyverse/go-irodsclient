package message

import (
	"golang.org/x/xerrors"
)

const (
	// RODS_MESSAGE_SSL_SHARED_SECRET_TYPE is a message type for shared secret used in SSL connection establishment
	RODS_MESSAGE_SSL_SHARED_SECRET_TYPE MessageType = "SHARED_SECRET"
)

// IRODSMessageSSLSharedSecret stores shared secret data
type IRODSMessageSSLSharedSecret struct {
	SharedSecret []byte
}

// NewIRODSMessageSSLSharedSecret creates a IRODSMessageSSLSharedSecret message
func NewIRODSMessageSSLSharedSecret(sharedSecret []byte) *IRODSMessageSSLSharedSecret {
	return &IRODSMessageSSLSharedSecret{
		SharedSecret: sharedSecret,
	}
}

// GetMessage builds a message
func (msg *IRODSMessageSSLSharedSecret) GetMessage() (*IRODSMessage, error) {
	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_SSL_SHARED_SECRET_TYPE,
		Message: msg.SharedSecret,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
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
func (msg *IRODSMessageSSLSharedSecret) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.SharedSecret = msgIn.Body.Message

	return nil
}

func (msg *IRODSMessageSSLSharedSecret) GetXMLCorrector() XMLCorrector {
	return nil
}
