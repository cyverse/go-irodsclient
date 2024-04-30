package message

import (
	"encoding/binary"

	"golang.org/x/xerrors"
)

// IRODSMessageResourceServerTransferEncryptionHeader stores resource server transfer encryption header message
type IRODSMessageResourceServerTransferEncryptionHeader struct {
	Length int
	IV     []byte

	ivSize int // internal, must be set before calling FromBytes
}

// NewIRODSMessageResourceServerTransferEncryptionHeader creates IRODSMessageResourceServerTransferEncryptionHeader
func NewIRODSMessageResourceServerTransferEncryptionHeader(ivSize int) *IRODSMessageResourceServerTransferEncryptionHeader {
	return &IRODSMessageResourceServerTransferEncryptionHeader{
		ivSize: ivSize,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageResourceServerTransferEncryptionHeader) GetBytes() ([]byte, error) {
	buf := make([]byte, 4+msg.ivSize)
	binary.LittleEndian.PutUint32(buf, uint32(msg.Length))
	copy(buf[4:4+msg.ivSize], msg.IV)
	return buf, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageResourceServerTransferEncryptionHeader) FromBytes(bytes []byte) error {
	if len(bytes) < 4+msg.ivSize {
		return xerrors.Errorf("failed to raed transfer encryption header, header must be %d bytes, but received %d", 4+msg.ivSize, len(bytes))
	}

	msg.Length = int(binary.LittleEndian.Uint32(bytes[0:4]))
	msg.IV = bytes[4 : 4+msg.ivSize]
	return nil
}

// SizeOf returns struct size in bytes
func (msg *IRODSMessageResourceServerTransferEncryptionHeader) SizeOf() int {
	return 4 + msg.ivSize
}
