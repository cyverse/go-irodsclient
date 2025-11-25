package main

import (
	"flag"
	"fmt"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/irods/connection"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{})

	log.SetLevel(log.DebugLevel)

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

	connConfig := &connection.IRODSConnectionConfig{
		ApplicationName: appName,
	}

	conn, err := connection.NewIRODSConnection(account, connConfig)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	err = conn.Connect()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
	defer func() {
		_ = conn.Disconnect()
	}()

	ver := conn.GetVersion()

	fmt.Printf("API Version: %s\n", ver.APIVersion)
	fmt.Printf("Release Version: %s\n", ver.ReleaseVersion)
}
