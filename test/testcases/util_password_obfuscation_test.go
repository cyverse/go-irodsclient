package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/sethvargo/go-password/password"
	"github.com/stretchr/testify/assert"
)

func getUtilPasswordObfuscationTest() Test {
	return Test{
		Name: "Util_PasswordObfuscation",
		Func: utilPasswordObfuscation,
	}
}

func utilPasswordObfuscation(t *testing.T, test *Test) {
	t.Run("StaticPasswords", testStaticPasswords)
	t.Run("RandomPasswords", testRandomPasswords)

}

func testStaticPasswords(t *testing.T) {
	obf := config.NewPasswordObfuscator()

	mypassword := "mypassword_1234_!@#$"
	encodedPassword := obf.Encode([]byte(mypassword))
	decodedPassword := obf.Decode(encodedPassword)
	assert.Equal(t, mypassword, string(decodedPassword))

	mypassword = "MicceLecos!@99"
	encodedPassword = obf.Encode([]byte(mypassword))
	decodedPassword = obf.Decode(encodedPassword)

	assert.Equal(t, mypassword, string(decodedPassword))

	for i := 0; i < 99; i++ {
		mypassword = fmt.Sprintf("loLLeooelef!@%d", i)
		encodedPassword := obf.Encode([]byte(mypassword))
		decodedPassword := obf.Decode(encodedPassword)

		assert.Equal(t, mypassword, string(decodedPassword))
	}
}

func testRandomPasswords(t *testing.T) {
	obf := config.NewPasswordObfuscator()

	for i := 0; i < 100000; i++ {
		mypassword, err := password.Generate(20, 10, 10, false, true)
		if err != nil {
			t.Fatal(err)
		}

		encodedPassword := obf.Encode([]byte(mypassword))
		decodedPassword := obf.Decode(encodedPassword)

		assert.Equal(t, mypassword, string(decodedPassword))
	}
}
