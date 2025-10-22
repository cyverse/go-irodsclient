package message

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// IRODSMessageResourceServerTransferHeader stores resource server transfer header message
type IRODSMessageResourceServerTransferHeader struct {
	OperationType int
	Flags         int
	Offset        int64
	Length        int64
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageResourceServerTransferHeader) FromBytes(bytes []byte) error {
	if len(bytes) < 24 {
		return errors.Errorf("failed to read transfer header, header must be 24 bytes, but received %d", len(bytes))
	}

	msg.OperationType = int(binary.BigEndian.Uint32(bytes[0:4]))
	msg.Flags = int(binary.BigEndian.Uint32(bytes[4:8]))
	msg.Offset = int64(binary.BigEndian.Uint64(bytes[8:16]))
	msg.Length = int64(binary.BigEndian.Uint64(bytes[16:24]))
	return nil
}

// SizeOf returns struct size in bytes
func (msg *IRODSMessageResourceServerTransferHeader) SizeOf() int {
	return 24
}
