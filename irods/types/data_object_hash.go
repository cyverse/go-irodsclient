package types

import (
	"encoding/hex"
	"fmt"
)

// ChecksumAlgorithm determines checksum algorithm
type ChecksumAlgorithm string

const (
	// ChecksumAlgorithmSHA256 is for SHA256
	ChecksumAlgorithmSHA256 ChecksumAlgorithm = "SHA256"
	// ChecksumAlgorithmSHA512 is for SHA512
	ChecksumAlgorithmSHA512 ChecksumAlgorithm = "SHA512"
	// ChecksumAlgorithmMD5 is for MD5
	ChecksumAlgorithmMD5 ChecksumAlgorithm = "MD5"
	// ChecksumAlgorithmUnknown is for unknown algorithm
	ChecksumAlgorithmUnknown ChecksumAlgorithm = ""
)

// IRODSChecksum contains data object hash information
type IRODSChecksum struct {
	Algorithm ChecksumAlgorithm
	Checksum  []byte
}

// ToString stringifies the object
func (checksum *IRODSChecksum) ToString() string {
	return fmt.Sprintf("<IRODSChecksum %s %x>", checksum.Algorithm, checksum.Checksum)
}

// GetChecksumString returns checksum in string
func (checksum *IRODSChecksum) GetChecksumString() string {
	return hex.EncodeToString(checksum.Checksum)
}
