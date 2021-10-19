package testcases

import (
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/test/server"

	log "github.com/sirupsen/logrus"
)

var (
	account *types.IRODSAccount
)

func setup() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setup",
	})

	var err error
	account, err = server.StartServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func shutdown() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "shutdown",
	})

	err := server.StopServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func GetTestAccount() *types.IRODSAccount {
	accountCpy := *account
	return &accountCpy
}
