package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

func main() {
	util.SetLogLevel(9)

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Give an iRODS path!\n")
		os.Exit(1)
	}

	inputPath := args[0]

	// Read account configuration from YAML file
	yaml, err := ioutil.ReadFile("account.yml")
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Account : %v", account.MaskSensitiveData())

	// Create a file system
	appName := "list_dir"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	defer filesystem.Release()

	entries, err := filesystem.List(inputPath)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	if len(entries) == 0 {
		fmt.Printf("Found no entries in the directory - %s\n", inputPath)
	} else {
		fmt.Printf("DIR: %s\n", inputPath)
		for _, entry := range entries {
			if entry.Type == fs.FSFileEntry {
				fmt.Printf("> FILE:\t%s\t%d\n", entry.Path, entry.Size)
			} else {
				// dir
				fmt.Printf("> DIRECTORY:\t%s\n", entry.Path)
			}

		}
	}
}
