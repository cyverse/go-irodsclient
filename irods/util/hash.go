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

func HashStrings(strs []string, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return GetHashStrings(strs, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return GetHashStrings(strs, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return GetHashStrings(strs, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return GetHashStrings(strs, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return GetHashStrings(strs, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %s", hashAlg)
	}
}

func HashLocalFile(sourcePath string, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return GetHashLocalFile(sourcePath, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return GetHashLocalFile(sourcePath, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return GetHashLocalFile(sourcePath, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return GetHashLocalFile(sourcePath, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return GetHashLocalFile(sourcePath, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %s", hashAlg)
	}
}

func HashBuffer(buffer bytes.Buffer, hashAlg string) ([]byte, error) {
	switch strings.ToLower(hashAlg) {
	case strings.ToLower(string(types.ChecksumAlgorithmMD5)):
		return GetHashBuffer(buffer, md5.New())
	case strings.ToLower(string(types.ChecksumAlgorithmADLER32)):
		return GetHashBuffer(buffer, adler32.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA1)):
		return GetHashBuffer(buffer, sha1.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA256)):
		return GetHashBuffer(buffer, sha256.New())
	case strings.ToLower(string(types.ChecksumAlgorithmSHA512)):
		return GetHashBuffer(buffer, sha512.New())
	default:
		return nil, xerrors.Errorf("unknown hash algorithm %s", hashAlg)
	}
}

func GetHashStrings(strs []string, hashAlg hash.Hash) ([]byte, error) {
	for _, str := range strs {
		_, err := hashAlg.Write([]byte(str))
		if err != nil {
			return nil, xerrors.Errorf("failed to write: %w", err)
		}
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}

func GetHashLocalFile(sourcePath string, hashAlg hash.Hash) ([]byte, error) {
	f, err := os.Open(sourcePath)
	if err != nil {
		return nil, xerrors.Errorf("failed to open file %s: %w", sourcePath, err)
	}

	defer f.Close()

	_, err = io.Copy(hashAlg, f)
	if err != nil {
		return nil, xerrors.Errorf("failed to write: %w", err)
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}

func GetHashBuffer(buffer bytes.Buffer, hashAlg hash.Hash) ([]byte, error) {
	_, err := hashAlg.Write(buffer.Bytes())
	if err != nil {
		return nil, xerrors.Errorf("failed to write: %w", err)
	}

	sumBytes := hashAlg.Sum(nil)
	return sumBytes, nil
}
