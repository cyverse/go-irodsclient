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
	appName := "delete_file"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	defer filesystem.Release()

	err = filesystem.RemoveFile(inputPath, true)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	if !filesystem.ExistsFile(inputPath) {
		fmt.Printf("Successfully deleted file %s\n", inputPath)
	} else {
		fmt.Printf("Could not delete file %s\n", inputPath)
	}
}
