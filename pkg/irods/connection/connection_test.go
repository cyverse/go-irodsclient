package connection

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/util"

	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

func TestIRODSConnection(t *testing.T) {

	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../../../config/test_account.yml")
	if err != nil {
		t.Errorf("err - %v", err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		t.Errorf("err - %v", err)
	}

	fmt.Printf("%v", account)

	timeout := time.Second * 20 // 20 sec
	conn := NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err = conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
	}

	ver := conn.GetVersion()
	fmt.Printf("%v", ver)
}

func TestIRODSConnectionWithNegotiation(t *testing.T) {

	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../../../config/test_account.yml")
	if err != nil {
		t.Errorf("err - %v", err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		t.Errorf("err - %v", err)
	}

	account.ClientServerNegotiation = true
	account.CSNegotiationPolicy = types.NEGOTIATION_USE_TCP

	fmt.Printf("%v", account)

	timeout := time.Second * 20 // 20 sec
	conn := NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err = conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
	}

	ver := conn.GetVersion()
	fmt.Printf("%v", ver)
}
