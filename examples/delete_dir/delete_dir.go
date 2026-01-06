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
	logger := log.WithFields(log.Fields{})

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
		fmt.Printf("Successfully deleted dir %q\n", inputPath)
	} else {
		fmt.Printf("Could not delete dir %q\n", inputPath)
	}
}
