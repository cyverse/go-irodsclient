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

// GetMessageHeader builds a message header
func (msg *IRODSMessageSSLSettings) GetMessageHeader() (*IRODSMessageHeader, error) {
	return &IRODSMessageHeader{
		Type:       MessageType(msg.EncryptionAlgorithm),
		MessageLen: msg.EncryptionKeySize,
		ErrorLen:   msg.SaltSize,
		BsLen:      msg.HashRounds,
		IntInfo:    0,
	}, nil
}
