package message

import (
	"encoding/binary"

	"golang.org/x/xerrors"
)

// IRODSMessageResourceServerTransferEncryptionHeader stores resource server transfer encryption header message
type IRODSMessageResourceServerTransferEncryptionHeader struct {
	Length int
	IV     []byte

	keySize int // internal, must be set before calling FromBytes
}

// NewIRODSMessageResourceServerTransferEncryptionHeader creates IRODSMessageResourceServerTransferEncryptionHeader
func NewIRODSMessageResourceServerTransferEncryptionHeader(keySize int) *IRODSMessageResourceServerTransferEncryptionHeader {
	return &IRODSMessageResourceServerTransferEncryptionHeader{
		keySize: keySize,
	}
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageResourceServerTransferEncryptionHeader) FromBytes(bytes []byte) error {
	if len(bytes) < 4+msg.keySize {
		return xerrors.Errorf("failed to raed transfer encryption header, header must be %d bytes, but received %d", 4+msg.keySize, len(bytes))
	}

	msg.Length = int(binary.BigEndian.Uint32(bytes[0:4]))
	msg.IV = bytes[4 : 4+msg.keySize]
	return nil
}

// SizeOf returns struct size in bytes
func (msg *IRODSMessageResourceServerTransferEncryptionHeader) SizeOf() int {
	return 4 + msg.keySize
}
