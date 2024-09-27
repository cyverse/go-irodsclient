package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/cyverse/go-irodsclient/fs"
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
	cfg, err := config.NewConfigFromYAMLFile(config.GetDefaultConfig(), "account.yml")
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account := cfg.ToIRODSAccount()
	logger.Debugf("Account : %v", account.GetRedacted())

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

	result, err := filesystem.DownloadFileParallelResumable(srcPath, "", destPath, 0, true, nil)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	logger.Infof("iRODS path: %q", result.IRODSPath)
	logger.Infof("Local path: %q", result.LocalPath)
	logger.Infof("Checksum: iRODS: %s:%q, Local: %s:%q", result.IRODSCheckSumAlgorithm, hex.EncodeToString(result.IRODSCheckSum), result.LocalCheckSumAlgorithm, hex.EncodeToString(result.LocalCheckSum))
	logger.Infof("Size: iRODS: %d, Local: %d", result.IRODSSize, result.LocalSize)

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
			fmt.Printf("Successfully downloaded a file %q to %q, size = %d\n", srcPath, destPath, fsinfo2.Size())
		} else {
			logger.Errorf("Unknown file type %q", fsinfo2.Mode())
		}
	} else {
		fmt.Printf("Successfully downloaded a file %q to %q, size = %d\n", srcPath, destPath, fsinfo.Size())
	}
}
