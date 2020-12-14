package main

import (
	"fmt"
	"os"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/api"
	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

func main() {

	account, err := types.CreateIRODSAccount(
		"data.cyverse.org",
		1247,
		"iychoi",
		"iplant",
		api.NATIVE_AUTH_SCHEME,
		"",
		"serverdn",
	)
	if err != nil {
		fmt.Printf("err - %v", err)
	}

	timeout := time.Second * 20 // 20 sec
	conn := connection.NewIRODSConnection(account, timeout, "go-irodsclient-test")
	conn.Connect()

	os.Exit(0)
}
