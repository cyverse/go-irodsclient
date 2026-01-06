package main

import (
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{})

	// Read account configuration from YAML file
	cfg := config.GetDefaultConfig()

	stat, err := os.Stat("account.yml")
	if err == nil && !stat.IsDir() {
		filecfg, err := config.NewConfigFromYAMLFile(cfg, "account.yml")
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		cfg = filecfg
	}

	// Read account configuration from ENV file
	envcfg, err := config.NewConfigFromEnv(cfg)
	if err == nil {
		cfg = envcfg
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	// Create a file system
	appName := "list_user"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	users, err := filesystem.ListUsers(account.ClientZone, types.IRODSUserRodsUser)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	fmt.Printf("Users:\n")

	if len(users) == 0 {
		fmt.Printf("Found no users\n")
	} else {
		for _, user := range users {
			fmt.Printf("> %d\n", user.ID)
			fmt.Printf("  id: %d\n", user.ID)
			fmt.Printf("  name: %s\n", user.Name)
			fmt.Printf("  zone: %s\n", user.Zone)
			fmt.Printf("  type: %s\n", user.Type)
		}
	}

	fmt.Printf("Groups:\n")

	groups, err := filesystem.ListUsers(account.ClientZone, types.IRODSUserRodsGroup)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if len(groups) == 0 {
		fmt.Printf("Found no groups\n")
	} else {
		for _, user := range groups {
			fmt.Printf("> %d\n", user.ID)
			fmt.Printf("  id: %d\n", user.ID)
			fmt.Printf("  name: %s\n", user.Name)
			fmt.Printf("  zone: %s\n", user.Zone)
			fmt.Printf("  type: %s\n", user.Type)
		}
	}

	fmt.Printf("Admin:\n")

	admins, err := filesystem.ListUsers(account.ClientZone, types.IRODSUserRodsAdmin)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if len(admins) == 0 {
		fmt.Printf("Found no admins\n")
	} else {
		for _, user := range admins {
			fmt.Printf("> %d\n", user.ID)
			fmt.Printf("  id: %d\n", user.ID)
			fmt.Printf("  name: %s\n", user.Name)
			fmt.Printf("  zone: %s\n", user.Zone)
			fmt.Printf("  type: %s\n", user.Type)
		}
	}

	fmt.Printf("Group Admin:\n")

	groupadmins, err := filesystem.ListUsers(account.ClientZone, types.IRODSUserGroupAdmin)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if len(groupadmins) == 0 {
		fmt.Printf("Found no groups admin\n")
	} else {
		for _, user := range groupadmins {
			fmt.Printf("> %d\n", user.ID)
			fmt.Printf("  id: %d\n", user.ID)
			fmt.Printf("  name: %s\n", user.Name)
			fmt.Printf("  zone: %s\n", user.Zone)
			fmt.Printf("  type: %s\n", user.Type)
		}
	}
}
