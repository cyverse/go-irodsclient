package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"

	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// Encrypt encrypts data
func Encrypt(algorithm types.EncryptionAlgorithm, key []byte, salt []byte, source []byte, dest []byte) error {
	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		return encryptAES256CBC(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256CTR:
		return encryptAES256CTR(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256CFB:
		return encryptAES256CFB(key, salt, source, dest)
	case types.EncryptionAlgorithmAES256OFB:
		return encryptAES256OFB(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CBC:
		return encryptDES256CBC(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CTR:
		return encryptDES256CTR(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256CFB:
		return encryptDES256CFB(key, salt, source, dest)
	case types.EncryptionAlgorithmDES256OFB:
		return encryptDES256OFB(key, salt, source, dest)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return xerrors.Errorf("unknown encryption algorithm")
	}
}

// Decrypt decrypts data
func Decrypt(algorithm types.EncryptionAlgorithm, key []byte, salt []byte, source []byte, dest []byte) error {
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
		return xerrors.Errorf("unknown encryption algorithm")
	}
}

func encryptAES256CBC(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	encrypter := cipher.NewCBCEncrypter(block, salt)
	encrypter.CryptBlocks(dest, source)

	return nil
}

func decryptAES256CBC(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return nil
}

func encryptAES256CTR(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptAES256CTR(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func encryptAES256CFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptAES256CFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func encryptAES256OFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptAES256OFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create AES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func encryptDES256CBC(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCEncrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return nil
}

func decryptDES256CBC(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCBCDecrypter(block, salt)
	decrypter.CryptBlocks(dest, source)

	return nil
}

func encryptDES256CTR(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptDES256CTR(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCTR(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func encryptDES256CFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBEncrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptDES256CFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewCFBDecrypter(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func encryptDES256OFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}

func decryptDES256OFB(key []byte, salt []byte, source []byte, dest []byte) error {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return xerrors.Errorf("failed to create DES cipher: %w", err)
	}

	decrypter := cipher.NewOFB(block, salt)
	decrypter.XORKeyStream(dest, source)

	return nil
}
