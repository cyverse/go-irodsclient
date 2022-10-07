package auth

import (
	"crypto/md5"
	"encoding/base64"

	"github.com/cyverse/go-irodsclient/irods/common"
)

const (
	challengeLen    int = 64
	authResponseLen int = 16
)

// GenerateAuthResponse returns auth response
func GenerateAuthResponse(challenge []byte, password string) string {
	paddedPassword := make([]byte, common.MaxPasswordLength)
	copy(paddedPassword, []byte(password))

	m := md5.New()
	m.Write(challenge[:challengeLen])
	m.Write(paddedPassword)
	encodedPassword := m.Sum(nil)

	// replace 0x00 to 0x01
	for idx := 0; idx < len(encodedPassword); idx++ {
		if encodedPassword[idx] == 0 {
			encodedPassword[idx] = 1
		}
	}

	b64encodedPassword := base64.StdEncoding.EncodeToString(encodedPassword[:authResponseLen])
	return b64encodedPassword
}
