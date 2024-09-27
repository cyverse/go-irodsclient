package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"
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

	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Give a ticket type and an iRODS path!\n")
		os.Exit(1)
	}

	ticketType := args[0]
	irodsPath := args[1]
	ticketName := ""
	if len(args) >= 3 {
		ticketName = args[2]
	}

	// Read account configuration from YAML file
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	// Create a file system
	appName := "create_ticket"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	err = filesystem.CreateTicket(ticketName, types.TicketType(ticketType), irodsPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}
