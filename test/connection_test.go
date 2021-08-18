package test

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/util"

	"github.com/cyverse/go-irodsclient/irods/types"
)

var (
	conn *connection.IRODSConnection
)

func setupConnection(requireCSNegotiation bool, csNegotiationPolicy types.CSNegotiationRequire) {
	setupTest()

	account.ClientServerNegotiation = requireCSNegotiation
	account.CSNegotiationPolicy = csNegotiationPolicy
	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn = connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}
}

func shutdownConnection() {
	conn.Disconnect()
	conn = nil
}

func TestIRODSConnection(t *testing.T) {
	setupConnection(false, types.CSNegotiationDontCare)

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)

	shutdownConnection()
}

func TestIRODSConnectionWithNegotiation(t *testing.T) {
	setupConnection(true, types.CSNegotiationRequireTCP)

	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	ver := conn.GetVersion()
	util.LogDebugf("Version : %v", ver)

	shutdownConnection()
}
