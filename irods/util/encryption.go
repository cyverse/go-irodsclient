package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"

	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// GetEncryptionBlockSize returns block size
func GetEncryptionBlockSize(algorithm types.EncryptionAlgorithm) int {
	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC, types.EncryptionAlgorithmAES256CTR, types.EncryptionAlgorithmAES256CFB, types.EncryptionAlgorithmAES256OFB:
		return 16
	case types.EncryptionAlgorithmDES256CBC, types.EncryptionAlgorithmDES256CTR, types.EncryptionAlgorithmDES256CFB, types.EncryptionAlgorithmDES256OFB:
		return 8
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0
	}
}

// Encrypt encrypts data
func Encrypt(algorithm types.EncryptionAlgorithm, key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	blockSize := GetEncryptionBlockSize(algorithm)
	paddedSource := padPkcs7(source, blockSize)

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		return encryptAES256CBC(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmAES256CTR:
		return encryptAES256CTR(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmAES256CFB:
		return encryptAES256CFB(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmAES256OFB:
		return encryptAES256OFB(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmDES256CBC:
		return encryptDES256CBC(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmDES256CTR:
		return encryptDES256CTR(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmDES256CFB:
		return encryptDES256CFB(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmDES256OFB:
		return encryptDES256OFB(key, salt, paddedSource, dest)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, xerrors.Errorf("unknown encryption algorithm")
	}
}

// Decrypt decrypts data
func Decrypt(algorithm types.EncryptionAlgorithm, key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		return decryptAES256CBC(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256CTR:
		return decryptAES256CTR(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256CFB:
		return decryptAES256CFB(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256OFB:
		return decryptAES256OFB(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CBC:
		return decryptDES256CBC(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CTR:
		return decryptDES256CTR(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CFB:
		return decryptDES256CFB(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256OFB:
		return decryptDES256OFB(key, salt, source, dest)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, xerrors.Errorf("unknown encryption algorithm")
	}
}

func padPkcs7(data []byte, blocksize int) []byte {
	if (len(data) % blocksize) == 0 {
		return data
	}

	n := blocksize - (len(data) % blocksize)
	pb := make([]byte, len(data)+n)
	copy(pb, data)
	copy(pb[len(data):], bytes.Repeat([]byte{byte(n)}, n))
	return pb
}

func encryptAES256CBC(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	encrypter := cipher.NewCBCEncrypter(block, salt)
	encrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func decryptAES256CBC(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func encryptAES256CTR(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256CTR(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptAES256CFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256CFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptAES256OFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256OFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256CBC(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCEncrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func decryptDES256CBC(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func encryptDES256CTR(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256CTR(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256CFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256CFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256OFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256OFB(key []byte, salt []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}
