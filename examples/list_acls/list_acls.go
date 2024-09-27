package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	log.SetLevel(log.DebugLevel)

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]

	// Read account configuration from YAML file
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

	// Create a file system
	appName := "list_acls"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	stat, err := filesystem.Stat(inputPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if stat.Type == fs.DirectoryEntry {
		inherit, err := filesystem.GetDirACLInheritance(inputPath)
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		if inherit.Inheritance {
			fmt.Printf("Inheritance - Enabled\n")
		} else {
			fmt.Printf("Inheritance - Disabled\n")
		}
	}

	accesses, err := filesystem.ListACLs(inputPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if len(accesses) == 0 {
		fmt.Printf("Found no acls for path %q\n", inputPath)
	} else {
		fmt.Printf("%s\n", inputPath)
		for _, access := range accesses {
			fmt.Printf("> User: %s (%s) = %s\n", access.UserName, access.UserType, access.AccessLevel)
		}
	}
}
