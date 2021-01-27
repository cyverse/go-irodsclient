package message

const (
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

// GetMessageBody builds a message body
func (msg *IRODSMessageSSLSharedSecret) GetMessageBody() (*IRODSMessageBody, error) {
	return &IRODSMessageBody{
		Type:    RODS_MESSAGE_SSL_SHARED_SECRET_TYPE,
		Message: msg.SharedSecret,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}, nil
}
