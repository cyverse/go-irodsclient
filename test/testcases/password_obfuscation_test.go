package testcases

import (
	"fmt"
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

	mypassword = "MicceLecos!@99"
	encodedPassword = icommands.EncodePasswordString(mypassword, 1000)
	decodedPassword = icommands.DecodePasswordString(encodedPassword, 1000)
	assert.Equal(t, mypassword, decodedPassword)

	for i := 0; i < 99; i++ {
		mypassword = fmt.Sprintf("loLLeooelef!@%d", i)
		encodedPassword = icommands.EncodePasswordString(mypassword, 1000)
		decodedPassword = icommands.DecodePasswordString(encodedPassword, 1000)

		//t.Logf("enc: %s", encodedPassword)
		//t.Logf("dec: %s", decodedPassword)

		assert.Equal(t, mypassword, decodedPassword)
	}

	//t.Logf("enc: %s", encodedPassword)
	//t.Logf("dec: %s", decodedPassword)
}
