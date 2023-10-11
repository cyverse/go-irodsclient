package types

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/xerrors"
)

// ChecksumAlgorithm determines checksum algorithm
type ChecksumAlgorithm string

const (
	// ChecksumAlgorithmSHA1 is for SHA1
	ChecksumAlgorithmSHA1 ChecksumAlgorithm = "SHA-1"
	// ChecksumAlgorithmSHA256 is for SHA256
	ChecksumAlgorithmSHA256 ChecksumAlgorithm = "SHA-256"
	// ChecksumAlgorithmSHA512 is for SHA512
	ChecksumAlgorithmSHA512 ChecksumAlgorithm = "SHA-512"
	// ChecksumAlgorithmADLER32 is for ADLER32
	ChecksumAlgorithmADLER32 ChecksumAlgorithm = "ADLER-32"
	// ChecksumAlgorithmMD5 is for MD5
	ChecksumAlgorithmMD5 ChecksumAlgorithm = "MD5"
	// ChecksumAlgorithmUnknown is for unknown algorithm
	ChecksumAlgorithmUnknown ChecksumAlgorithm = ""
)

// IRODSChecksum contains data object hash information
type IRODSChecksum struct {
	OriginalChecksum string
	Algorithm        ChecksumAlgorithm
	Checksum         []byte
}

// CreateIRODSChecksum creates IRODSChecksum from checksum string
func CreateIRODSChecksum(checksumString string) (*IRODSChecksum, error) {
	algorithm, checksum, err := ParseIRODSChecksum(checksumString)
	if err != nil {
		return nil, xerrors.Errorf("failed to split data object checksum: %w", err)
	}

	return &IRODSChecksum{
		OriginalChecksum: checksumString,
		Algorithm:        algorithm,
		Checksum:         checksum,
	}, nil
}

// ToString stringifies the object
func (checksum *IRODSChecksum) ToString() string {
	return fmt.Sprintf("<IRODSChecksum %s %x>", checksum.Algorithm, checksum.Checksum)
}

// GetChecksumString returns checksum in string
func (checksum *IRODSChecksum) GetChecksumString() string {
	return hex.EncodeToString(checksum.Checksum)
}

// GetOriginalChecksum returns original checksum in string
func (checksum *IRODSChecksum) GetOriginalChecksum() string {
	return checksum.OriginalChecksum
}

// GetHashAlgorithm returns checksum algorithm
func (checksum *IRODSChecksum) GetChecksumAlgorithm() string {
	return string(checksum.Algorithm)
}

// ParseIRODSChecksum parses iRODS checksum string
func ParseIRODSChecksum(checksumString string) (ChecksumAlgorithm, []byte, error) {
	sp := strings.Split(checksumString, ":")

	if len(sp) == 0 || len(sp) > 2 {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unexpected checksum: %v", string(checksumString))
	}

	if len(sp) == 1 {
		checksum, err := base64.StdEncoding.DecodeString(checksumString)
		if err != nil {
			return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("failed to base64 decode checksum: %v", err)
		}

		return ChecksumAlgorithmMD5, checksum, nil
	}

	algorithm := sp[0]
	checksum, err := base64.StdEncoding.DecodeString(sp[1])
	if err != nil {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("failed to base64 decode checksum: %v", err)
	}

	switch strings.ToLower(algorithm) {
	case "sha2":
		if len(checksum) == 256/8 {
			return ChecksumAlgorithmSHA256, checksum, nil
		} else if len(checksum) == 512/8 {
			return ChecksumAlgorithmSHA512, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	case "sha256":
		if len(checksum) == 256/8 {
			return ChecksumAlgorithmSHA256, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	case "sha512":
		if len(checksum) == 512/8 {
			return ChecksumAlgorithmSHA512, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	case "sha1":
		if len(checksum) == 160/8 {
			return ChecksumAlgorithmSHA1, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	case "adler32":
		if len(checksum) == 32/8 {
			return ChecksumAlgorithmADLER32, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	case "md5":
		if len(checksum) == 128/8 {
			return ChecksumAlgorithmMD5, checksum, nil
		}
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	default:
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	}
}
