package util

import "crypto/md5"

const wheel = `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!"#$%&'()*+,-./`

// Scramble implements the obfEncodeByKey irods cpp function.
func Scramble(in, key string) string {
	// Key buffer
	keyBuf := make([]byte, 100)

	for i := 0; i < len(key) && i < 100; i++ {
		keyBuf[i] = byte(key[i])
	}

	buffer := make([]byte, 64)

	hash := md5.Sum(keyBuf)
	copy(buffer, hash[:])

	// Hash of the hash
	hash = md5.Sum(buffer[0:16])
	copy(buffer[16:], hash[:])

	// Hash of 2 hashes
	hash = md5.Sum(buffer[0:32])
	copy(buffer[32:], hash[:])

	// Hash of 2 hashes
	hash = md5.Sum(buffer[0:32])
	copy(buffer[48:], hash[:])

	var out = []byte{}

	for p := 0; p < len(in); p++ {
		k := int(buffer[p%61])

		var found bool

		for i := 0; i < len(wheel); i++ {
			if in[p] == wheel[i] {
				j := (i + k) % len(wheel)
				out = append(out, wheel[j])
				found = true
				break
			}
		}

		if !found {
			out = append(out, in[p])
		}
	}

	return string(out)
}
