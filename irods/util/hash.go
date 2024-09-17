package util

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"hash/adler32"
	"io"
	"os"
	"strings"

	"golang.org/x/xerrors"

	"github.com/cyverse/go-irodsclient/irods/types"
)

// HashStrings calculates hash of strings
func HashStrings(strs []string, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return HashStringsWithAlgorithm(strs, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return HashStringsWithAlgorithm(strs, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return HashStringsWithAlgorithm(strs, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return HashStringsWithAlgorithm(strs, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return HashStringsWithAlgorithm(strs, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %q", hashAlg)
	}
}

// HashLocalFile calculates hash of local file
func HashLocalFile(sourcePath string, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return HashLocalFileWithAlgorithm(sourcePath, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return HashLocalFileWithAlgorithm(sourcePath, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return HashLocalFileWithAlgorithm(sourcePath, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return HashLocalFileWithAlgorithm(sourcePath, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return HashLocalFileWithAlgorithm(sourcePath, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %q", hashAlg)
	}
}

// HashBuffer calculates hash of buffer data
func HashBuffer(buffer *bytes.Buffer, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return HashBufferWithAlgorithm(buffer, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return HashBufferWithAlgorithm(buffer, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return HashBufferWithAlgorithm(buffer, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return HashBufferWithAlgorithm(buffer, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return HashBufferWithAlgorithm(buffer, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %q", hashAlg)
	}
}

// HashStringsWithAlgorithm calculates hash of strings
func HashStringsWithAlgorithm(strs []string, hashAlg hash.Hash) ([]byte, error) {
	for _, str := range strs {
		_, err := hashAlg.Write([]byte(str))
		if err != nil {
			return nil, xerrors.Errorf("failed to write: %w", err)
		}
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}

// HashLocalFileWithAlgorithm calculates hash of local file
func HashLocalFileWithAlgorithm(sourcePath string, hashAlg hash.Hash) ([]byte, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		return nil, xerrors.Errorf("failed to open file %q: %w", sourcePath, err)
	}

	defer f.Close()

	_, err = io.Copy(hashAlg, f)
	if err != nil {
		return nil, xerrors.Errorf("failed to write: %w", err)
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}

// HashBufferWithAlgorithm calculates hash of buffer data
func HashBufferWithAlgorithm(buffer *bytes.Buffer, hashAlg hash.Hash) ([]byte, error) {
	_, err := hashAlg.Write(buffer.Bytes())
	if err != nil {
		return nil, xerrors.Errorf("failed to write: %w", err)
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}
