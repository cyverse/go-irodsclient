package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
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
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS ticket!\n")
		os.Exit(1)
	}

	ticketName := args[0]

	// Read account configuration from YAML file
	yaml, err := os.ReadFile("account.yml")
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
	appName := "get_ticket"
	config := fs.NewFileSystemConfigWithDefault(appName)
	sessConfig := session.NewIRODSSessionConfig(config.ApplicationName, config.ConnectionLifespan, config.OperationTimeout, config.ConnectionIdleTimeout, config.ConnectionMax, config.StartNewTransaction)
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer sess.Release()

	/*
		conn, err := sess.AcquireConnection()
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		ticket, err := irods_fs.GetTicket(conn, ticketName)
		if err != nil {
			logger.Error(err)
			panic(err)
		}
	*/
	fmt.Printf("> Ticket: %s\n", ticketName)
	/*
		fmt.Printf("%s\n", ticket.ToString())
	*/
}
