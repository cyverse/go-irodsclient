package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// Parse cli parameters
	flag.Parse()

	// Read account configuration from YAML file
	yaml, err := ioutil.ReadFile("account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	logger.Debugf("Account : %v", account.MaskSensitiveData())

	// Create a file system
	appName := "version"

	conn := connection.NewIRODSConnection(account, 5*time.Minute, appName)
	conn.Connect()
	defer conn.Disconnect()

	ver := conn.GetVersion()

	fmt.Printf("API Version: %s\n", ver.APIVersion)
	fmt.Printf("Release Version: %s\n", ver.ReleaseVersion)
}
