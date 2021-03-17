package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

func main() {
	util.SetLogLevel(9)

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "Give a local source path and an iRODS destination path!\n")
		os.Exit(1)
	}

	srcPath := args[0]
	destPath := args[1]

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
	appName := "upload"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	defer filesystem.Release()

	// convert src path into absolute path
	srcPath, err = filepath.Abs(srcPath)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	err = filesystem.UploadFile(srcPath, destPath, "", false)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	fsentry, err := filesystem.Stat(destPath)
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	if fsentry.Type == fs.FSFileEntry {
		fmt.Printf("Successfully uploaded a file %s to %s, size = %d\n", srcPath, destPath, fsentry.Size)
	} else {
		// dir
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destFilePath := util.MakeIRODSPath(destPath, srcFileName)

		fsentry2, err := filesystem.Stat(destFilePath)
		if err != nil {
			util.LogErrorf("err - %v", err)
			panic(err)
		}

		if fsentry2.Type == fs.FSFileEntry {
			fmt.Printf("Successfully uploaded a file %s to %s, size = %d\n", srcPath, destFilePath, fsentry2.Size)
		} else {
			util.LogErrorf("Unkonwn file type - %s", fsentry2.Type)
		}
	}
}
