package types

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

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

	algorithm, checksum, err := ParseIRODSChecksum(checksumString)
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

// ParseIRODSChecksum parses iRODS checksum string
func ParseIRODSChecksum(checksumString string) (ChecksumAlgorithm, []byte, error) {
	sp := strings.Split(checksumString, ":")

	if len(sp) == 0 || len(sp) > 2 {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unexpected checksum: %v", string(checksumString))
	}

	algorithm := ""
	checksum := ""

	if len(sp) == 1 {
		algorithm = string(ChecksumAlgorithmMD5)
		checksum = checksumString
	} else if len(sp) >= 2 {
		algorithm = sp[0]
		checksum = sp[1]
	}

	var checksumBytes []byte
	var err error
	if strings.HasPrefix(strings.ToLower(algorithm), "sha") {
		// sha-x algorithms are encoded with base64
		checksumBytes, err = base64.StdEncoding.DecodeString(checksum)
		if err != nil {
			return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("failed to base64 decode checksum: %v", err)
		}
	} else {
		// hex encoded
		checksumBytes, err = hex.DecodeString(checksum)
		if err != nil {
			return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("failed to hex decode checksum: %v", err)
		}
	}

	if strings.ToLower(algorithm) == "sha2" {
		// sha256 or sha512
		if len(checksumBytes) == GetChecksumDigestSize(ChecksumAlgorithmSHA256) {
			return ChecksumAlgorithmSHA256, checksumBytes, nil
		} else if len(checksumBytes) == GetChecksumDigestSize(ChecksumAlgorithmSHA512) {
			return ChecksumAlgorithmSHA512, checksumBytes, nil
		}
	}

	checksumAlgorithm := GetChecksumAlgorithm(algorithm)
	if checksumAlgorithm == ChecksumAlgorithmUnknown {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksumBytes))
	}

	if len(checksumBytes) != GetChecksumDigestSize(checksumAlgorithm) {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksumBytes))
	}

	return checksumAlgorithm, checksumBytes, nil
}
