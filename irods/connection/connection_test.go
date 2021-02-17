package connection

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/util"

	"github.com/cyverse/go-irodsclient/irods/types"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
	conn    *IRODSConnection
)

func setup(requireCSNegotiation bool, csNegotiationPolicy types.CSNegotiationRequire) {
	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../../../config/test_account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	timeout = time.Second * 20 // 20 sec

	account.ClientServerNegotiation = requireCSNegotiation
	account.CSNegotiationPolicy = csNegotiationPolicy
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn = NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err = conn.Connect()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}
}

func shutdown() {
	conn.Disconnect()
	conn = nil
}

func TestIRODSConnection(t *testing.T) {
	setup(false, types.CSNegotiationDontCare)

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)

	shutdown()
}

func TestIRODSConnectionWithNegotiation(t *testing.T) {
	setup(true, types.CSNegotiationRequireTCP)

	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)

	shutdown()
}
