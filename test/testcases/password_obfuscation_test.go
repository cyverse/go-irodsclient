package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/utils/icommands"
	"github.com/stretchr/testify/assert"
)

func TestPasswordObfuscation(t *testing.T) {
	t.Run("test PasswordObfuscation", testEncodeDecodePassword)
}

func testEncodeDecodePassword(t *testing.T) {
	mypassword := "mypassword_1234_!@#$"
	encodedPassword := icommands.EncodePasswordString(mypassword, 2345)
	decodedPassword := icommands.DecodePasswordString(encodedPassword, 2345)
	assert.Equal(t, mypassword, decodedPassword)
}
