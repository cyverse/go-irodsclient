package message

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
