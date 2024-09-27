package main

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// Read account configuration from YAML file
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

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
			restrictions, err := filesystem.GetTicketRestrictions(ticket.ID)
			if err != nil {
				logger.Error(err)
				panic(err)
			}

			fmt.Printf("> %d\n", ticket.ID)
			fmt.Printf("  id: %d\n", ticket.ID)
			fmt.Printf("  name: %s\n", ticket.Name)
			fmt.Printf("  type: %s\n", ticket.Type)
			fmt.Printf("  owner: %s\n", ticket.Owner)
			fmt.Printf("  owner zone: %s\n", ticket.OwnerZone)
			fmt.Printf("  object type: %s\n", ticket.ObjectType)
			fmt.Printf("  path: %s\n", ticket.Path)
			fmt.Printf("  expireTime: %s\n", ticket.ExpirationTime)
			fmt.Printf("  uses limit: %d\n", ticket.UsesLimit)
			fmt.Printf("  uses count: %d\n", ticket.UsesCount)
			fmt.Printf("  write file limit: %d\n", ticket.WriteFileLimit)
			fmt.Printf("  write file count: %d\n", ticket.WriteFileCount)
			fmt.Printf("  write byte limit: %d\n", ticket.WriteByteLimit)
			fmt.Printf("  write byte count: %d\n", ticket.WriteByteCount)

			if len(restrictions.AllowedHosts) == 0 {
				fmt.Printf("  No host restrictions\n")
			} else {
				for _, host := range restrictions.AllowedHosts {
					fmt.Printf("  host restriction: %s\n", host)
				}
			}

			if len(restrictions.AllowedUserNames) == 0 {
				fmt.Printf("  No user restrictions\n")
			} else {
				for _, user := range restrictions.AllowedUserNames {
					fmt.Printf("  user restriction: %s\n", user)
				}
			}

			if len(restrictions.AllowedGroupNames) == 0 {
				fmt.Printf("  No group restrictions\n")
			} else {
				for _, group := range restrictions.AllowedGroupNames {
					fmt.Printf("  group restriction: %s\n", group)
				}
			}
		}
	}
}
