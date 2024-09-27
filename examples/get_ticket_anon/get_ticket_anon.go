package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"

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
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	// Create a file system
	appName := "get_ticket_anon"
	config := fs.NewFileSystemConfig(appName)
	sessConfig := config.ToMetadataSessionConfig()
	sess, err := session.NewIRODSSession(account, sessConfig)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer sess.Release()

	conn, err := sess.AcquireConnection()
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	ticket, err := irods_fs.GetTicketForAnonymousAccess(conn, ticketName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	/*
		filesystem, err := fs.NewFileSystemWithDefault(account, appName)
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		defer filesystem.Release()


		entries, err := filesystem.List(inputPath)
		if err != nil {
			logger.Error(err)
			panic(err)
		}
	*/

	fmt.Printf("> Ticket: %s\n", ticketName)
	fmt.Printf("%s\n", ticket.ToString())
}
