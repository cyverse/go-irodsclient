package auth

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
)

const (
	challengeLen    int = 64
	authResponseLen int = 16
)

// GenerateAuthResponse returns auth response
func GenerateAuthResponse(challenge string, password string) (string, error) {
	challengeBytes, err := base64.StdEncoding.DecodeString(challenge)
	if err != nil {
		return "", fmt.Errorf("Could not decode an authentication challenge")
	}

	paddedPassword := make([]byte, common.MaxPasswordLength, common.MaxPasswordLength)
	copy(paddedPassword, []byte(password))

	m := md5.New()
	m.Write(challengeBytes[:challengeLen])
	m.Write(paddedPassword)
	encodedPassword := m.Sum(nil)

	// replace 0x00 to 0x01
	for idx := 0; idx < len(encodedPassword); idx++ {
		if encodedPassword[idx] == 0 {
			encodedPassword[idx] = 1
		}
	}

	b64encodedPassword := base64.StdEncoding.EncodeToString(encodedPassword[:authResponseLen])
	return b64encodedPassword, nil
}
