package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"

	log "github.com/sirupsen/logrus"
)

func main() {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// Parse cli parameters
	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "Give an iRODS source path and a local destination path!\n")
		os.Exit(1)
	}

	srcPath := args[0]
	destPath := args[1]

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
	appName := "download"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	// convert dest path into absolute path
	destPath, err = filepath.Abs(destPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	outputChan, errChan := filesystem.DownloadFileParallelInBlocksAsync(srcPath, "", destPath, 0, 0)
	outputDone := false
	errDone := false
	for !outputDone || !errDone {
		select {
		case offset, ok := <-outputChan:
			if ok {
				logger.Infof("done reading data at offset %d", offset)
			} else {
				outputDone = true
			}
		case err, ok := <-errChan:
			if ok {
				logger.Error(err)
				panic(err)
			} else {
				errDone = true
			}
		}
	}

	fsinfo, err := os.Stat(destPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if fsinfo.IsDir() {
		// dir
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destFilePath := util.MakeIRODSPath(destPath, srcFileName)

		fsinfo2, err := os.Stat(destFilePath)
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		if !fsinfo2.IsDir() {
			fmt.Printf("Successfully downloaded a file %s to %s, size = %d\n", srcPath, destPath, fsinfo2.Size())
		} else {
			logger.Errorf("Unkonwn file type - %s", fsinfo2.Mode())
		}
	} else {
		fmt.Printf("Successfully downloaded a file %s to %s, size = %d\n", srcPath, destPath, fsinfo.Size())
	}
}
