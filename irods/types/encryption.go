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

// GetChecksumAlgorithm returns checksum algorithm from string
func GetChecksumAlgorithm(checksumAlgorithm string) ChecksumAlgorithm {
	switch strings.ToLower(checksumAlgorithm) {
	case "sha256", strings.ToLower(string(ChecksumAlgorithmSHA256)):
		return ChecksumAlgorithmSHA256
	case "sha512", strings.ToLower(string(ChecksumAlgorithmSHA512)):
		return ChecksumAlgorithmSHA512
	case "sha1", strings.ToLower(string(ChecksumAlgorithmSHA1)):
		return ChecksumAlgorithmSHA1
	case "adler32", strings.ToLower(string(ChecksumAlgorithmADLER32)):
		return ChecksumAlgorithmADLER32
	case strings.ToLower(string(ChecksumAlgorithmMD5)):
		return ChecksumAlgorithmMD5
	default:
		return ChecksumAlgorithmUnknown
	}
}

// GetChecksumDigestSize returns checksum digest size
func GetChecksumDigestSize(checksumAlgorithm ChecksumAlgorithm) int {
	switch checksumAlgorithm {
	case ChecksumAlgorithmSHA256:
		return 256 / 8
	case ChecksumAlgorithmSHA512:
		return 512 / 8
	case ChecksumAlgorithmSHA1:
		return 160 / 8
	case ChecksumAlgorithmADLER32:
		return 32 / 8
	case ChecksumAlgorithmMD5:
		return 128 / 8
	default:
		return 0
	}
}

// ParseIRODSChecksumString parses iRODS checksum string
func ParseIRODSChecksumString(checksumString string) (ChecksumAlgorithm, []byte, error) {
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
		// sha256
		if len(checksumBytes) == GetChecksumDigestSize(ChecksumAlgorithmSHA256) {
			return ChecksumAlgorithmSHA256, checksumBytes, nil
		} else if len(checksumBytes) == GetChecksumDigestSize(ChecksumAlgorithmSHA512) {
			return ChecksumAlgorithmSHA512, checksumBytes, nil
		}
	} else if strings.ToLower(algorithm) == "sha512" {
		if len(checksumBytes) == GetChecksumDigestSize(ChecksumAlgorithmSHA512) {
			return ChecksumAlgorithmSHA512, checksumBytes, nil
		}
	}

	checksumAlgorithm := GetChecksumAlgorithm(algorithm)
	if checksumAlgorithm == ChecksumAlgorithmUnknown {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %q len %d", algorithm, len(checksumBytes))
	}

	if len(checksumBytes) != GetChecksumDigestSize(checksumAlgorithm) {
		return ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %q len %d", algorithm, len(checksumBytes))
	}

	return checksumAlgorithm, checksumBytes, nil
}

// MakeIRODSChecksumString makes iRODS checksum string
func MakeIRODSChecksumString(algorithm ChecksumAlgorithm, checksum []byte) (string, error) {
	if strings.HasPrefix(strings.ToLower(string(algorithm)), "sha") {
		// sha-x algorithms are encoded with base64
		checksumString := base64.StdEncoding.EncodeToString(checksum)

		switch algorithm {
		case ChecksumAlgorithmSHA1:
			return fmt.Sprintf("%s:%s", "sha1", checksumString), nil
		case ChecksumAlgorithmSHA256:
			return fmt.Sprintf("%s:%s", "sha2", checksumString), nil
		case ChecksumAlgorithmSHA512:
			return fmt.Sprintf("%s:%s", "sha512", checksumString), nil
		default:
			return "", xerrors.Errorf("unknown algorithm %q", algorithm)
		}
	}

	// hex encoded
	checksumString := hex.EncodeToString(checksum)

	switch algorithm {
	case ChecksumAlgorithmADLER32:
		return fmt.Sprintf("%s:%s", "adler32", checksumString), nil
	case ChecksumAlgorithmMD5:
		return checksumString, nil
	default:
		return "", xerrors.Errorf("unknown algorithm %q", algorithm)
	}
}

// EncryptionAlgorithm determines encryption algorithm
type EncryptionAlgorithm string

const (
	// EncryptionAlgorithmAES256CBC is for AES-256-CBC
	EncryptionAlgorithmAES256CBC EncryptionAlgorithm = "AES-256-CBC"
	// EncryptionAlgorithmAES256CTR is for AES-256-CTR
	EncryptionAlgorithmAES256CTR EncryptionAlgorithm = "AES-256-CTR"
	// EncryptionAlgorithmAES256CFB is for AES-256-CFB
	EncryptionAlgorithmAES256CFB EncryptionAlgorithm = "AES-256-CFB"
	// EncryptionAlgorithmAES256OFB is for AES-256-OFB
	EncryptionAlgorithmAES256OFB EncryptionAlgorithm = "AES-256-OFB"
	// EncryptionAlgorithmDES256CBC is for DES-256-CBC
	EncryptionAlgorithmDES256CBC EncryptionAlgorithm = "DES-256-CBC"
	// EncryptionAlgorithmDES256CTR is for DES-256-CTR
	EncryptionAlgorithmDES256CTR EncryptionAlgorithm = "DES-256-CTR"
	// EncryptionAlgorithmDES256CFB is for DES-256-CFB
	EncryptionAlgorithmDES256CFB EncryptionAlgorithm = "DES-256-CFB"
	// EncryptionAlgorithmDES256OFB is for DES-256-OFB
	EncryptionAlgorithmDES256OFB EncryptionAlgorithm = "DES-256-OFB"
	// EncryptionAlgorithmUnknown is for unknown algorithm
	EncryptionAlgorithmUnknown EncryptionAlgorithm = ""
)

// GetEncryptionAlgorithm returns encryption algorithm from string
func GetEncryptionAlgorithm(encryptionAlgorithm string) EncryptionAlgorithm {
	switch strings.ToLower(encryptionAlgorithm) {
	case "aes256cbc", strings.ToLower(string(EncryptionAlgorithmAES256CBC)):
		return EncryptionAlgorithmAES256CBC
	case "aes256ctr", strings.ToLower(string(EncryptionAlgorithmAES256CTR)):
		return EncryptionAlgorithmAES256CTR
	case "aes256cfb", strings.ToLower(string(EncryptionAlgorithmAES256CFB)):
		return EncryptionAlgorithmAES256CFB
	case "aes256ofb", strings.ToLower(string(EncryptionAlgorithmAES256OFB)):
		return EncryptionAlgorithmAES256OFB
	case "des256cbc", strings.ToLower(string(EncryptionAlgorithmDES256CBC)):
		return EncryptionAlgorithmDES256CBC
	case "des256ctr", strings.ToLower(string(EncryptionAlgorithmDES256CTR)):
		return EncryptionAlgorithmDES256CTR
	case "des256cfb", strings.ToLower(string(EncryptionAlgorithmDES256CFB)):
		return EncryptionAlgorithmDES256CFB
	case "des256ofb", strings.ToLower(string(EncryptionAlgorithmDES256OFB)):
		return EncryptionAlgorithmDES256OFB
	default:
		return EncryptionAlgorithmUnknown
	}
}
