package testcases

import (
	"encoding/hex"
	"testing"

	irods_util "github.com/cyverse/go-irodsclient/irods/util"
	"github.com/stretchr/testify/assert"
)

func getUtilEncodingTest() Test {
	return Test{
		Name: "Util_Encoding",
		Func: utilEncodingTest,
	}
}

func utilEncodingTest(t *testing.T, test *Test) {
	t.Run("EncoderRing", testEncoderRing)
	t.Run("Scramble", testScramble)
	t.Run("ClientSignature", testClientSignature)
}

func testEncoderRing(t *testing.T) {
	ring := irods_util.GetEncoderRing("def")
	ringHex := hex.EncodeToString(ring)
	assert.Equal(t, "5fbabc5bfd2ef4f4d65024d364c3241a71c71aae827a91b654e9de55e62f3cb23840a894e36c7149ddd8963a1b228df43840a894e36c7149ddd8963a1b228df4", ringHex)
}

func testScramble(t *testing.T) {
	scrPass1 := irods_util.Scramble(";.ObfV2test_password", "06fed401fb79f864272a421835486736", "", false)
	assert.Equal(t, ";EBo$tJuoAY_RigHonj-", scrPass1)

	scrPass2 := irods_util.Scramble(";.ObfV2test_password", "06fed401fb79f864272a421835486736", "", true)
	assert.Equal(t, ";E3O&GDl4!&_$3GBd+B\"", scrPass2)
}

func testClientSignature(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	conn, err := sess.AcquireConnection(true)
	FailError(t, err)

	signature := conn.GetClientSignature()
	assert.Equal(t, 32, len(signature))
}
