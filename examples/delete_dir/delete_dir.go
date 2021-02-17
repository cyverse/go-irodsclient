package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/iychoi/go-irodsclient/fs"
	"github.com/iychoi/go-irodsclient/irods/types"
	"github.com/iychoi/go-irodsclient/irods/util"
)

func main() {
	util.SetLogLevel(9)

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
	appName := "delete_dir"
	filesystem := fs.NewFileSystemWithDefault(account, appName)
	defer filesystem.Release()

	err = filesystem.RemoveDir(inputPath, recurse, true)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	if !filesystem.ExistsDir(inputPath) {
		fmt.Printf("Successfully deleted dir %s\n", inputPath)
	} else {
		fmt.Printf("Could not delete dir %s\n", inputPath)
	}
}
