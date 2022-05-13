package icommands

import (
	"io/ioutil"
	"os"
	"time"
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

	//defaultPasswordKey    string = "a9_3fker"
	//defaultScramblePrefix string = ".E_"
	//v2Prefix              string = "A.ObfV2"
)

// DecodePasswordFile decodes password string in .irodsA file
func DecodePasswordFile(path string, uid int) (string, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	return DecodePasswordString(string(content), uid), nil
}

// EncodePasswordFile encodes password string and store in .irodsA file
func EncodePasswordFile(path string, s string, uid int) error {
	content := EncodePasswordString(s, uid)
	return ioutil.WriteFile(path, []byte(content), 0600)
}

// DecodePasswordString decodes password string in .irodsA file
func DecodePasswordString(encodedPassword string, uid int) string {
	s := []byte(encodedPassword)

	// This value lets us know which seq value to use
	// Referred to as "rval" in the C code
	seqIndex := s[6] - 'e'
	seq := seqList[seqIndex]

	// How much we bitshift seq by when we use it
	// Referred to as "addin_i" in the C code
	// Since we're skipping five bytes that are normally read,
	// we start at 15
	bitshift := 15

	// The uid is used as a salt.
	if uid <= 0 {
		uid = os.Getuid()
	}

	// The first byte is a dot, the next five are literally irrelevant
	// garbage, and we already used the seventh one. The string to decode
	// starts at byte eight.
	encodedString := s[7:]
	decodedString := []byte{}

	uidOffset := uid & 0xf5f

	for _, c := range encodedString {
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

				decodedString = append(decodedString, wheel[newWheelIndex])
				foundInWheel = true
				break
			}
		}

		if !foundInWheel {
			decodedString = append(decodedString, c)
		}
	}
	return string(decodedString)
}

// EncodePasswordString encodes password string to be stored in .irodsA file
func EncodePasswordString(s string, uid int) string {
	// mtime & 65535 needs to be within 20 seconds of the
	// .irodsA file's mtime & 65535
	mtime := (time.Now().UnixMicro() / 1000) & 0xf

	// How much we bitshift seq by when we use it
	// Referred to as "addin_i" in the C code
	// We can't skip the first five bytes this time,
	// so we start at 0
	bitshift := 0

	// The uid is used as a salt.
	if uid <= 0 {
		uid = os.Getuid()
	}

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
	toEncode = append(toEncode, []byte(s)...)

	// Yeah, the string starts with a dot. Whatever.
	encodedString := []byte{'.'}
	uidOffset := uid & 0xf5f

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

				encodedString = append(encodedString, wheel[newWheelIndex])
				foundInWheel = true
				break
			}
		}

		if !foundInWheel {
			encodedString = append(encodedString, c)
		}
	}

	// insert the seq_index (which is NOT encoded):
	encodedStringPartA := string(encodedString[:6])
	encodedStringPartB := string(byte(seqIndex + 'e'))
	encodedStringPartC := string(encodedString[6:])

	encodedString = []byte(encodedStringPartA + encodedStringPartB + encodedStringPartC)

	// aaaaand, append a null character. because we want to print
	// a null character to the file. because that's a good idea.
	encodedString = append(encodedString, 0)
	return string(encodedString)
}
