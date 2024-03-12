package types

import (
	"strings"
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
