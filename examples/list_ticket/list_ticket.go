package main

import (
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

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
	appName := "list_ticket"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	tickets, err := filesystem.ListTickets()
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if len(tickets) == 0 {
		fmt.Printf("Found no tickets\n")
	} else {
		for _, ticket := range tickets {
			fmt.Printf("> %d\n", ticket.ID)
			fmt.Printf("  id: %d\n", ticket.ID)
			fmt.Printf("  name: %s\n", ticket.Name)
			fmt.Printf("  type: %s\n", ticket.Type)
			fmt.Printf("  owner: %s\n", ticket.Owner)
			fmt.Printf("  owner zone: %s\n", ticket.OwnerZone)
			fmt.Printf("  object type: %s\n", ticket.ObjectType)
			fmt.Printf("  path: %s\n", ticket.Path)
			fmt.Printf("  expireTime: %s\n", ticket.ExpireTime)
			fmt.Printf("  uses limit: %d\n", ticket.UsesLimit)
			fmt.Printf("  uses count: %d\n", ticket.UsesCount)
			fmt.Printf("  write file limit: %d\n", ticket.WriteFileLimit)
			fmt.Printf("  write file count: %d\n", ticket.WriteFileCount)
			fmt.Printf("  write byte limit: %d\n", ticket.WriteByteLimit)
			fmt.Printf("  write byte count: %d\n", ticket.WriteByteCount)
		}
	}
}
