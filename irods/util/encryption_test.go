package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/types"
)

var encryptionTestAlgorithms = []types.EncryptionAlgorithm{
	types.EncryptionAlgorithmAES256CBC,
	types.EncryptionAlgorithmAES256CTR,
	types.EncryptionAlgorithmAES256CFB,
	types.EncryptionAlgorithmAES256OFB,
	types.EncryptionAlgorithmDES256CBC,
	types.EncryptionAlgorithmDES256CTR,
	types.EncryptionAlgorithmDES256CFB,
	types.EncryptionAlgorithmDES256OFB,
}

// des.NewCipher only accepts an 8 byte key, aes.NewCipher a 32 byte one for AES-256
func encryptionTestKey(algorithm types.EncryptionAlgorithm) []byte {
	keyLen := 32
	switch algorithm {
	case types.EncryptionAlgorithmDES256CBC, types.EncryptionAlgorithmDES256CTR,
		types.EncryptionAlgorithmDES256CFB, types.EncryptionAlgorithmDES256OFB:
		keyLen = 8
	}

	key := make([]byte, keyLen)
	for i := range key {
		key[i] = byte(i + 1)
	}

	return key
}

func encryptionTestIV() []byte {
	iv := make([]byte, 32)
	for i := range iv {
		iv[i] = byte(0xA0 + i)
	}

	return iv
}

func encryptionTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 11)
	}

	return data
}

// referencePadPkcs7 is the padding the previous implementation produced, kept here so that the
// tests pin the wire format rather than only checking that Encrypt and Decrypt agree
func referencePadPkcs7(data []byte, blockSize int) []byte {
	padLen := blockSize - (len(data) % blockSize)
	ref := bytes.Repeat([]byte{byte(padLen)}, padLen)
	pb := make([]byte, len(data)+padLen)

	copy(pb, data)
	copy(pb[len(data):], ref)
	return pb
}

// referenceEncrypt encrypts the same way the previous implementation did, straight on top of the
// standard library, into a freshly allocated destination
func referenceEncrypt(t *testing.T, algorithm types.EncryptionAlgorithm, key []byte, iv []byte, source []byte) []byte {
	t.Helper()

	blockSize := GetEncryptionBlockSize(algorithm)
	padded := referencePadPkcs7(source, blockSize)
	dest := make([]byte, len(padded))

	var block cipher.Block
	var err error

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC, types.EncryptionAlgorithmAES256CTR,
		types.EncryptionAlgorithmAES256CFB, types.EncryptionAlgorithmAES256OFB:
		block, err = aes.NewCipher(key)
	default:
		block, err = des.NewCipher(key)
	}
	if err != nil {
		t.Fatal(err)
	}

	switch algorithm {
	case types.EncryptionAlgorithmAES256CBC, types.EncryptionAlgorithmDES256CBC:
		cipher.NewCBCEncrypter(block, iv[:blockSize]).CryptBlocks(dest, padded)
	case types.EncryptionAlgorithmAES256CTR, types.EncryptionAlgorithmDES256CTR:
		cipher.NewCTR(block, iv[:blockSize]).XORKeyStream(dest, padded)
	case types.EncryptionAlgorithmAES256CFB, types.EncryptionAlgorithmDES256CFB:
		cipher.NewCFBEncrypter(block, iv[:blockSize]).XORKeyStream(dest, padded) //nolint:staticcheck
	case types.EncryptionAlgorithmAES256OFB, types.EncryptionAlgorithmDES256OFB:
		cipher.NewOFB(block, iv[:blockSize]).XORKeyStream(dest, padded) //nolint:staticcheck
	default:
		t.Fatalf("unhandled algorithm %v", algorithm)
	}

	return dest
}

func TestEncryptMatchesPreviousOutput(t *testing.T) {
	iv := encryptionTestIV()

	for _, algorithm := range encryptionTestAlgorithms {
		blockSize := GetEncryptionBlockSize(algorithm)
		key := encryptionTestKey(algorithm)

		for _, size := range []int{0, 1, blockSize - 1, blockSize, blockSize + 1, 1000, 4096} {
			source := encryptionTestData(size)

			want := referenceEncrypt(t, algorithm, key, iv, source)

			dest := make([]byte, size+blockSize)
			gotLen, err := Encrypt(algorithm, key, iv, source, dest)
			if err != nil {
				t.Fatalf("%v, size %d: %v", algorithm, size, err)
			}

			if gotLen != len(want) {
				t.Fatalf("%v, size %d: encrypted length %d, want %d", algorithm, size, gotLen, len(want))
			}

			if !bytes.Equal(dest[:gotLen], want) {
				t.Fatalf("%v, size %d: ciphertext differs from the previous implementation", algorithm, size)
			}
		}
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	iv := encryptionTestIV()

	for _, algorithm := range encryptionTestAlgorithms {
		blockSize := GetEncryptionBlockSize(algorithm)
		key := encryptionTestKey(algorithm)

		for _, size := range []int{0, 1, blockSize - 1, blockSize, blockSize + 1, 1000, 4096} {
			source := encryptionTestData(size)

			encrypted := make([]byte, size+blockSize)
			encryptedLen, err := Encrypt(algorithm, key, iv, source, encrypted)
			if err != nil {
				t.Fatalf("%v, size %d: %v", algorithm, size, err)
			}

			decrypted := make([]byte, encryptedLen)
			decryptedLen, err := Decrypt(algorithm, key, iv, encrypted[:encryptedLen], decrypted)
			if err != nil {
				t.Fatalf("%v, size %d: %v", algorithm, size, err)
			}

			if !bytes.Equal(decrypted[:decryptedLen], source) {
				t.Fatalf("%v, size %d: round trip mismatch, got %d bytes, want %d", algorithm, size, decryptedLen, size)
			}
		}
	}
}

func TestEncryptDoesNotTouchDestinationBeyondResult(t *testing.T) {
	algorithm := types.EncryptionAlgorithmAES256CBC
	blockSize := GetEncryptionBlockSize(algorithm)
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := encryptionTestData(100)

	// a destination larger than needed, as the resource server transfer uses a reused buffer
	dest := make([]byte, 1024)
	for i := range dest {
		dest[i] = 0xEE
	}

	encryptedLen, err := Encrypt(algorithm, key, iv, source, dest)
	if err != nil {
		t.Fatal(err)
	}

	if encryptedLen != len(source)+blockSize-(len(source)%blockSize) {
		t.Fatalf("unexpected encrypted length %d", encryptedLen)
	}

	for i := encryptedLen; i < len(dest); i++ {
		if dest[i] != 0xEE {
			t.Fatalf("destination was modified past the result at %d", i)
		}
	}
}

func TestEncryptRejectsSmallDestination(t *testing.T) {
	algorithm := types.EncryptionAlgorithmAES256CBC
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := encryptionTestData(64)

	// exactly the source length leaves no room for the padding block
	if _, err := Encrypt(algorithm, key, iv, source, make([]byte, len(source))); err == nil {
		t.Fatal("expected Encrypt to reject a destination with no room for padding")
	}
}

func TestDecryptRejectsSmallDestination(t *testing.T) {
	algorithm := types.EncryptionAlgorithmAES256CBC
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := encryptionTestData(64)

	encrypted := make([]byte, len(source)+GetEncryptionBlockSize(algorithm))
	encryptedLen, err := Encrypt(algorithm, key, iv, source, encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Decrypt(algorithm, key, iv, encrypted[:encryptedLen], make([]byte, encryptedLen-1)); err == nil {
		t.Fatal("expected Decrypt to reject a destination smaller than the source")
	}
}

func TestDecryptRejectsInvalidPadding(t *testing.T) {
	algorithm := types.EncryptionAlgorithmAES256CTR
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := encryptionTestData(64)

	encrypted := make([]byte, len(source)+GetEncryptionBlockSize(algorithm))
	encryptedLen, err := Encrypt(algorithm, key, iv, source, encrypted)
	if err != nil {
		t.Fatal(err)
	}

	// CTR is a stream cipher, so flipping a ciphertext byte flips the same plaintext byte
	encrypted[encryptedLen-1] ^= 0xFF

	if _, err := Decrypt(algorithm, key, iv, encrypted[:encryptedLen], make([]byte, encryptedLen)); err == nil {
		t.Fatal("expected Decrypt to reject invalid padding")
	}
}

func TestEncryptDecryptUnknownAlgorithm(t *testing.T) {
	key := encryptionTestKey(types.EncryptionAlgorithmAES256CBC)
	iv := encryptionTestIV()

	source := encryptionTestData(64)
	dest := make([]byte, 128)

	if _, err := Encrypt(types.EncryptionAlgorithmUnknown, key, iv, source, dest); err == nil {
		t.Fatal("expected Encrypt to reject an unknown algorithm")
	}

	if _, err := Decrypt(types.EncryptionAlgorithmUnknown, key, iv, source, dest); err == nil {
		t.Fatal("expected Decrypt to reject an unknown algorithm")
	}
}

func BenchmarkEncrypt4MB(b *testing.B) {
	algorithm := types.EncryptionAlgorithmAES256CBC
	blockSize := GetEncryptionBlockSize(algorithm)
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := make([]byte, 4*1024*1024)
	dest := make([]byte, len(source)+blockSize)

	b.SetBytes(int64(len(source)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := Encrypt(algorithm, key, iv, source, dest); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecrypt4MB(b *testing.B) {
	algorithm := types.EncryptionAlgorithmAES256CBC
	blockSize := GetEncryptionBlockSize(algorithm)
	key := encryptionTestKey(algorithm)
	iv := encryptionTestIV()

	source := make([]byte, 4*1024*1024)
	encrypted := make([]byte, len(source)+blockSize)

	encryptedLen, err := Encrypt(algorithm, key, iv, source, encrypted)
	if err != nil {
		b.Fatal(err)
	}

	dest := make([]byte, encryptedLen)

	b.SetBytes(int64(encryptedLen))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := Decrypt(algorithm, key, iv, encrypted[:encryptedLen], dest); err != nil {
			b.Fatal(err)
		}
	}
}
