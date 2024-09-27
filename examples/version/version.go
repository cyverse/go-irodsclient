package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/irods/connection"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	logger.Logger.SetLevel(log.DebugLevel)

	// Parse cli parameters
	flag.Parse()

	// Read account configuration from YAML file
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	// Create a file system
	appName := "version"

	conn := connection.NewIRODSConnection(account, 5*time.Minute, appName)
	err = conn.Connect()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
	defer conn.Disconnect()

	ver := conn.GetVersion()

	fmt.Printf("API Version: %s\n", ver.APIVersion)
	fmt.Printf("Release Version: %s\n", ver.ReleaseVersion)
}
