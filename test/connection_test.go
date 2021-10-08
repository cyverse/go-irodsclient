package test

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

var (
	conn *connection.IRODSConnection
)

func setupConnection(requireCSNegotiation bool, csNegotiationPolicy types.CSNegotiationRequire) {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setupConnection",
	})

	setupTest()

	account.ClientServerNegotiation = requireCSNegotiation
	account.CSNegotiationPolicy = csNegotiationPolicy
	logger.Debugf("Account : %v", account.MaskSensitiveData())

	conn = connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}
}

func shutdownConnection() {
	conn.Disconnect()
	conn = nil
}

func TestIRODSConnection(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "TestIRODSConnection",
	})

	setupConnection(false, types.CSNegotiationDontCare)

	ver := conn.GetVersion()
	logger.Debugf("Version : %v", ver)

	shutdownConnection()
}

func TestIRODSConnectionWithNegotiation(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "TestIRODSConnectionWithNegotiation",
	})

	setupConnection(true, types.CSNegotiationRequireTCP)

	logger.Debugf("Account : %v", account.MaskSensitiveData())

	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	err := conn.Connect()
	if err != nil {
		t.Errorf("err - %v", err)
		panic(err)
	}

	ver := conn.GetVersion()
	logger.Debugf("Version : %v", ver)

	shutdownConnection()
}
