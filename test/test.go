package test

import (
	"io/ioutil"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

var (
	account *types.IRODSAccount
	timeout time.Duration
)

func setupTest() {
	util.SetLogLevel(9)

	yaml, err := ioutil.ReadFile("../config/test_account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err = types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	timeout = time.Second * 200 // 200 sec
}
