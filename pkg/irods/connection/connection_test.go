package connection

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/util"

	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
)

func init() {
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
}

func TestIRODSConnection(t *testing.T) {
	account.ClientServerNegotiation = false
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
	}

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)
}

func TestIRODSConnectionWithNegotiation(t *testing.T) {
	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.CSNegotiationRequireTCP
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
	}

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)
}
