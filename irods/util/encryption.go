package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/rand"

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

// GetEncryptionIV returns a new IV
func GetEncryptionIV(algorithm types.EncryptionAlgorithm) ([]byte, error) {
	blockSize := GetEncryptionBlockSize(algorithm)
	iv := make([]byte, blockSize)
	_, err := rand.Read(iv)
	if err != nil {
		return nil, xerrors.Errorf("failed to generate iv: %w", err)
	}

	return iv, nil
}

// Encrypt encrypts data
func Encrypt(algorithm types.EncryptionAlgorithm, key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	blockSize := GetEncryptionBlockSize(algorithm)
	paddedSource := padPkcs7(source, blockSize)

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		return encryptAES256CBC(key, iv[:blockSize], paddedSource, dest)
	case types.EncryptionAlgorithmAES256CTR:
		return encryptAES256CTR(key, iv[:blockSize], paddedSource, dest)
	case types.EncryptionAlgorithmAES256CFB:
		return encryptAES256CFB(key, iv[:blockSize], paddedSource, dest)
	case types.EncryptionAlgorithmAES256OFB:
		return encryptAES256OFB(key, iv[:blockSize], paddedSource, dest)
	case types.EncryptionAlgorithmDES256CBC:
		return encryptDES256CBC(key, iv[:8], paddedSource, dest)
	case types.EncryptionAlgorithmDES256CTR:
		return encryptDES256CTR(key, iv[:8], paddedSource, dest)
	case types.EncryptionAlgorithmDES256CFB:
		return encryptDES256CFB(key, iv[:8], paddedSource, dest)
	case types.EncryptionAlgorithmDES256OFB:
		return encryptDES256OFB(key, iv[:8], paddedSource, dest)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, xerrors.Errorf("unknown encryption algorithm")
	}
}

// Decrypt decrypts data
func Decrypt(algorithm types.EncryptionAlgorithm, key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	blockSize := GetEncryptionBlockSize(algorithm)

	var err error
	paddedDest := make([]byte, len(source))

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		_, err = decryptAES256CBC(key, iv[:blockSize], source, paddedDest)
	case types.EncryptionAlgorithmAES256CTR:
		_, err = decryptAES256CTR(key, iv[:blockSize], source, paddedDest)
	case types.EncryptionAlgorithmAES256CFB:
		_, err = decryptAES256CFB(key, iv[:blockSize], source, paddedDest)
	case types.EncryptionAlgorithmAES256OFB:
		_, err = decryptAES256OFB(key, iv[:blockSize], source, paddedDest)
	case types.EncryptionAlgorithmDES256CBC:
		_, err = decryptDES256CBC(key, iv[:8], source, paddedDest)
	case types.EncryptionAlgorithmDES256CTR:
		_, err = decryptDES256CTR(key, iv[:8], source, paddedDest)
	case types.EncryptionAlgorithmDES256CFB:
		_, err = decryptDES256CFB(key, iv[:8], source, paddedDest)
	case types.EncryptionAlgorithmDES256OFB:
		_, err = decryptDES256OFB(key, iv[:8], source, paddedDest)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, xerrors.Errorf("unknown encryption algorithm")
	}

	if err != nil {
		return 0, err
	}

	unpaddedDest, err := stripPkcs7(paddedDest, blockSize)
	if err != nil {
		return 0, xerrors.Errorf("failed to strip pkcs7 padding: %w", err)
	}

	destLen := copy(dest, unpaddedDest)
	return destLen, nil
}

func padPkcs7(data []byte, blocksize int) []byte {
	padLen := blocksize - (len(data) % blocksize)
	ref := bytes.Repeat([]byte{byte(padLen)}, padLen)
	pb := make([]byte, len(data)+padLen)

	copy(pb, data)
	copy(pb[len(data):], ref)
	return pb
}

func stripPkcs7(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if (len(data) % blockSize) != 0 {
		return nil, xerrors.Errorf("unaligned data")
	}

	padLen := int(data[len(data)-1])
	ref := bytes.Repeat([]byte{byte(padLen)}, padLen)
	if padLen > blockSize {
		return nil, xerrors.Errorf("invalid pkcs7 padding, padding length %d is larger than block size %d", padLen, blockSize)
	}

	if padLen == 0 {
		return nil, xerrors.Errorf("invalid pkcs7 padding, padding length must be non-zero")
	}

	if !bytes.HasSuffix(data, ref) {
		return nil, xerrors.Errorf("invalid pkcs7 padding")
	}
	return data[:len(data)-padLen], nil
}

func encryptAES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	encrypter := cipher.NewCBCEncrypter(block, iv)
	encrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func decryptAES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func encryptAES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptAES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptAES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptAES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCEncrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func decryptDES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

func encryptDES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func encryptDES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

func decryptDES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}
