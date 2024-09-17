package config

import (
	"os"
	"time"

	"golang.org/x/xerrors"
)

var (
	seqList = []int64{
		0xd768b678,
		0xedfdaf56,
		0x2420231b,
		0x987098d8,
		0xc1bdfeee,
		0xf572341f,
		0x478def3a,
		0xa830d343,
		0x774dfa2a,
		0x6720731e,
		0x346fa320,
		0x6ffdf43a,
		0x7723a320,
		0xdf67d02e,
		0x86ad240a,
		0xe76d342e,
	}

	wheel = []byte{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
		'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',
	}
)

// PasswordObfuscator obfuscate icommands password
type PasswordObfuscator struct {
	UID int
}

// NewPasswordObfuscator creates a new PasswordObfuscator
func NewPasswordObfuscator() *PasswordObfuscator {
	return &PasswordObfuscator{
		UID: os.Getuid(),
	}
}

// SetUID sets UID for seeding
func (obf *PasswordObfuscator) SetUID(uid int) {
	obf.UID = uid
}

// DecodeFile decodes password string in a file
func (obf *PasswordObfuscator) DecodeFile(path string) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, xerrors.Errorf("failed to read file %q: %w", path, err)
	}

	return obf.Decode(content), nil
}

// EncodeToFile encodes password string and stores it in a file
func (obf *PasswordObfuscator) EncodeToFile(path string, password []byte) error {
	content := obf.Encode(password)

	err := os.WriteFile(path, []byte(content), 0600)
	if err != nil {
		return xerrors.Errorf("failed to write file %q: %w", path, err)
	}
	return nil
}

// Decode decodes password
func (obf *PasswordObfuscator) Decode(encodedPassword []byte) []byte {
	// This value lets us know which seq value to use
	// Referred to as "rval" in the C code
	seqIndex := encodedPassword[6] - 'e'
	seq := seqList[seqIndex]

	// How much we bitshift seq by when we use it
	// Referred to as "addin_i" in the C code
	// Since we're skipping five bytes that are normally read,
	// we start at 15
	bitshift := 15

	// The first byte is a dot, the next five are literally irrelevant
	// garbage, and we already used the seventh one. The string to decode
	// starts at byte eight.
	encodedBytes := encodedPassword[7:]
	decodedBytes := []byte{}

	uidOffset := obf.UID & 0xf5f

	for _, c := range encodedBytes {
		if c == 0 {
			break
		}

		// How far this character is from the target character in wheel
		// Referred to as "add_in" in the C code
		offset := int((seq>>bitshift)&0x1f) + uidOffset

		bitshift += 3
		if bitshift > 28 {
			bitshift = 0
		}

		// The character is only encoded if it's one of the ones in wheel
		foundInWheel := false
		for wheelIndex, wheelChar := range wheel {
			if wheelChar == c {
				// index of the target character in wheel
				newWheelIndex := wheelIndex - offset
				for newWheelIndex < 0 {
					newWheelIndex += len(wheel)
				}

				decodedBytes = append(decodedBytes, wheel[newWheelIndex])
				foundInWheel = true
				break
			}
		}

		if !foundInWheel {
			decodedBytes = append(decodedBytes, c)
		}
	}
	return decodedBytes
}

// Encode encodes password
func (obf *PasswordObfuscator) Encode(password []byte) []byte {
	// mtime & 65535 needs to be within 20 seconds of the
	// .irodsA file's mtime & 65535
	mtime := time.Now().Unix()

	// How much we bitshift seq by when we use it
	// Referred to as "addin_i" in the C code
	// We can't skip the first five bytes this time,
	// so we start at 0
	bitshift := 0

	//This value lets us know which seq value to use
	// Referred to as "rval" in the C code
	// The C code is very specific about this being mtime & 15,
	// but it's never checked. Let's use zero.
	seqIndex := 0
	seq := seqList[seqIndex]

	toEncode := []byte{}

	// The C code DOES really care about this value matching
	// the seq_index, though
	toEncode = append(toEncode, byte('S'-((seqIndex&0x7)*2)))

	// And this is also a song and dance to
	// convince the C code we are legitimate
	toEncode = append(toEncode, byte('a'+((mtime>>4)&0xf)))
	toEncode = append(toEncode, byte('a'+(mtime&0xf)))
	toEncode = append(toEncode, byte('a'+((mtime>>12)&0xf)))
	toEncode = append(toEncode, byte('a'+((mtime>>8)&0xf)))

	// We also want to actually encode the passed string
	toEncode = append(toEncode, []byte(password)...)

	// Yeah, the string starts with a dot. Whatever.
	encodedBytes := []byte{'.'}
	uidOffset := obf.UID & 0xf5f

	for _, c := range toEncode {
		if c == 0 {
			break
		}

		// How far this character is from the target character in wheel
		// Referred to as "add_in" in the C code
		offset := int((seq>>bitshift)&0x1f) + uidOffset

		bitshift += 3
		if bitshift > 28 {
			bitshift = 0
		}

		// The character is only encoded if it's one of the ones in wheel
		foundInWheel := false
		for wheelIndex, wheelChar := range wheel {
			if wheelChar == c {
				// index of the target character in wheel
				newWheelIndex := wheelIndex + offset
				for newWheelIndex < 0 {
					newWheelIndex += len(wheel)
				}

				newWheelIndex %= len(wheel)

				encodedBytes = append(encodedBytes, wheel[newWheelIndex])
				foundInWheel = true
				break
			}
		}

		if !foundInWheel {
			encodedBytes = append(encodedBytes, c)
		}
	}

	// insert the seq_index (which is NOT encoded):
	encodedStringPartA := string(encodedBytes[:6])
	encodedStringPartB := string(byte(seqIndex + 'e'))
	encodedStringPartC := string(encodedBytes[6:])

	encodedBytes = []byte(encodedStringPartA + encodedStringPartB + encodedStringPartC)

	// aaaaand, append a null character. because we want to print
	// a null character to the file. because that's a good idea.
	encodedBytes = append(encodedBytes, 0)
	return encodedBytes
}
