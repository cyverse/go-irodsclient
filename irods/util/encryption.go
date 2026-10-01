package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/rand"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/types"
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
		return nil, errors.Wrapf(err, "failed to generate iv")
	}

	return iv, nil
}

// Encrypt encrypts source into dest and returns the encrypted length.
// dest must hold the source plus up to one block of pkcs7 padding, and must not overlap source.
func Encrypt(algorithm types.EncryptionAlgorithm, key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	blockSize := GetEncryptionBlockSize(algorithm)
	if blockSize == 0 {
		return 0, errors.Errorf("unknown encryption algorithm")
	}

	// pad into dest and encrypt it in place, to avoid allocating and copying a padded
	// copy of the source for every block
	paddedLen, err := padPkcs7(dest, source, blockSize)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to add pkcs7 padding")
	}

	padded := dest[:paddedLen]

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		return encryptAES256CBC(key, iv[:blockSize], padded, padded)
	case types.EncryptionAlgorithmAES256CTR:
		return encryptAES256CTR(key, iv[:blockSize], padded, padded)
	case types.EncryptionAlgorithmAES256CFB:
		return encryptAES256CFB(key, iv[:blockSize], padded, padded)
	case types.EncryptionAlgorithmAES256OFB:
		return encryptAES256OFB(key, iv[:blockSize], padded, padded)
	case types.EncryptionAlgorithmDES256CBC:
		return encryptDES256CBC(key, iv[:8], padded, padded)
	case types.EncryptionAlgorithmDES256CTR:
		return encryptDES256CTR(key, iv[:8], padded, padded)
	case types.EncryptionAlgorithmDES256CFB:
		return encryptDES256CFB(key, iv[:8], padded, padded)
	case types.EncryptionAlgorithmDES256OFB:
		return encryptDES256OFB(key, iv[:8], padded, padded)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, errors.Errorf("unknown encryption algorithm")
	}
}

// Decrypt decrypts source into dest and returns the decrypted length.
// dest must be at least as large as source, as the padding is only dropped after decryption.
func Decrypt(algorithm types.EncryptionAlgorithm, key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	blockSize := GetEncryptionBlockSize(algorithm)
	if blockSize == 0 {
		return 0, errors.Errorf("unknown encryption algorithm")
	}

	if len(dest) < len(source) {
		return 0, errors.Errorf("destination buffer is too small, %d < %d", len(dest), len(source))
	}

	// decrypt straight into dest and drop the padding by shortening the result, to avoid
	// allocating a padded buffer and copying out of it for every block
	padded := dest[:len(source)]

	var err error

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC:
		_, err = decryptAES256CBC(key, iv[:blockSize], source, padded)
	case types.EncryptionAlgorithmAES256CTR:
		_, err = decryptAES256CTR(key, iv[:blockSize], source, padded)
	case types.EncryptionAlgorithmAES256CFB:
		_, err = decryptAES256CFB(key, iv[:blockSize], source, padded)
	case types.EncryptionAlgorithmAES256OFB:
		_, err = decryptAES256OFB(key, iv[:blockSize], source, padded)
	case types.EncryptionAlgorithmDES256CBC:
		_, err = decryptDES256CBC(key, iv[:8], source, padded)
	case types.EncryptionAlgorithmDES256CTR:
		_, err = decryptDES256CTR(key, iv[:8], source, padded)
	case types.EncryptionAlgorithmDES256CFB:
		_, err = decryptDES256CFB(key, iv[:8], source, padded)
	case types.EncryptionAlgorithmDES256OFB:
		_, err = decryptDES256OFB(key, iv[:8], source, padded)
	case types.EncryptionAlgorithmUnknown:
		fallthrough
	default:
		return 0, errors.Errorf("unknown encryption algorithm")
	}

	if err != nil {
		return 0, err
	}

	destLen, err := stripPkcs7(padded, blockSize)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to strip pkcs7 padding")
	}

	return destLen, nil
}

// padPkcs7 copies data into dest, appends pkcs7 padding and returns the padded length
func padPkcs7(dest []byte, data []byte, blockSize int) (int, error) {
	padLen := blockSize - (len(data) % blockSize)
	paddedLen := len(data) + padLen

	if len(dest) < paddedLen {
		return 0, errors.Errorf("destination buffer is too small, %d < %d", len(dest), paddedLen)
	}

	copy(dest, data)

	for i := len(data); i < paddedLen; i++ {
		dest[i] = byte(padLen)
	}

	return paddedLen, nil
}

// stripPkcs7 validates the pkcs7 padding of data and returns the unpadded length
func stripPkcs7(data []byte, blockSize int) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if (len(data) % blockSize) != 0 {
		return 0, errors.Errorf("unaligned data")
	}

	padLen := int(data[len(data)-1])
	if padLen > blockSize {
		return 0, errors.Errorf("invalid pkcs7 padding, padding length %d is larger than block size %d", padLen, blockSize)
	}

	if padLen == 0 {
		return 0, errors.Errorf("invalid pkcs7 padding, padding length must be non-zero")
	}

	for _, b := range data[len(data)-padLen:] {
		if b != byte(padLen) {
			return 0, errors.Errorf("invalid pkcs7 padding")
		}
	}

	return len(data) - padLen, nil
}

//nolint:all
func encryptAES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	encrypter := cipher.NewCBCEncrypter(block, iv)
	encrypter.CryptBlocks(dest, source)

	return len(source), nil
}

//nolint:all
func decryptAES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

//nolint:all
func encryptAES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptAES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func encryptAES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewCFBEncrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptAES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewCFBDecrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func encryptAES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptAES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create AES cipher")
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func encryptDES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCBCEncrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

//nolint:all
func decryptDES256CBC(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(dest, source)

	return len(source), nil
}

//nolint:all
func encryptDES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptDES256CTR(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCTR(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func encryptDES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCFBEncrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptDES256CFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewCFBDecrypter(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func encryptDES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}

//nolint:all
func decryptDES256OFB(key []byte, iv []byte, source []byte, dest []byte) (int, error) {
	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create DES cipher")
	}

	decrypter := cipher.NewOFB(block, iv)
	decrypter.XORKeyStream(dest, source)

	return len(source), nil
}
