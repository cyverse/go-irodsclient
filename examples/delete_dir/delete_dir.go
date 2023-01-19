package main

import (
	"flag"
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

	recurse := false
	// Parse cli parameters
	flag.BoolVar(&recurse, "recurse", false, "recursive")
	flag.BoolVar(&recurse, "r", false, "recursive")
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]

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
	appName := "delete_dir"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	err = filesystem.RemoveDir(inputPath, recurse, true)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if !filesystem.ExistsDir(inputPath) {
		fmt.Printf("Successfully deleted dir %s\n", inputPath)
	} else {
		fmt.Printf("Could not delete dir %s\n", inputPath)
	}
}
