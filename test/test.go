package test

import (
	"io/ioutil"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
)

func setupTest() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setupTest",
	})

	yaml, err := ioutil.ReadFile("../config/test_account.yml")
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	timeout = time.Second * 200 // 200 sec
}
