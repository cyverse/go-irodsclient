package types

import (
	"fmt"

	"golang.org/x/xerrors"
)

// IRODSChecksum contains data object hash information
type IRODSChecksum struct {
	IRODSChecksumString string
	Algorithm           ChecksumAlgorithm
	Checksum            []byte
}

// CreateIRODSChecksum creates IRODSChecksum from checksum string
func CreateIRODSChecksum(checksumString string) (*IRODSChecksum, error) {
	if len(checksumString) == 0 {
		// completely normal
		return &IRODSChecksum{
			IRODSChecksumString: "",
			Algorithm:           "",
			Checksum:            nil,
		}, nil
	}

	algorithm, checksum, err := ParseIRODSChecksumString(checksumString)
	if err != nil {
		return nil, xerrors.Errorf("failed to split data object checksum: %w", err)
	}

	return &IRODSChecksum{
		IRODSChecksumString: checksumString,
		Algorithm:           algorithm,
		Checksum:            checksum,
	}, nil
}

// ToString stringifies the object
func (checksum *IRODSChecksum) ToString() string {
	return fmt.Sprintf("<IRODSChecksum %s %x>", checksum.Algorithm, checksum.Checksum)
}
