package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		fmt.Fprintf(os.Stderr, "Give a local source path and an iRODS destination path!\n")
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
	appName := "upload"
	filesystem, err := fs.NewFileSystemWithDefault(account, appName)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	defer filesystem.Release()

	// convert src path into absolute path
	srcPath, err = filepath.Abs(srcPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	fmt.Printf("upload start")

	track := func(processed int64, total int64) {
		pt := (float64(processed) / float64(total)) * 100
		fmt.Printf("%d / %d ==> %f%%\n", processed, total, pt)
	}

	tstart := time.Now()

	result, err := filesystem.UploadFile(srcPath, destPath, "", false, true, true, track)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	logger.Infof("iRODS path: %q", result.IRODSPath)
	logger.Infof("Local path: %q", result.LocalPath)
	logger.Infof("Checksum: iRODS: %s:%q, Local: %s:%q", result.IRODSCheckSumAlgorithm, hex.EncodeToString(result.IRODSCheckSum), result.LocalCheckSumAlgorithm, hex.EncodeToString(result.LocalCheckSum))
	logger.Infof("Size: iRODS: %d, Local: %d", result.IRODSSize, result.LocalSize)

	ttaken := time.Since(tstart)

	fsentry, err := filesystem.Stat(destPath)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	if fsentry.Type == fs.FileEntry {
		fmt.Printf("Successfully uploaded a file %q to %q, size = %d\n", srcPath, destPath, fsentry.Size)
	} else {
		// dir
		srcFileName := util.GetIRODSPathFileName(srcPath)
		destFilePath := util.MakeIRODSPath(destPath, srcFileName)

		fsentry2, err := filesystem.Stat(destFilePath)
		if err != nil {
			logger.Error(err)
			panic(err)
		}

		if fsentry2.Type == fs.FileEntry {
			fmt.Printf("Successfully uploaded a file %q to %q, size = %d\n", srcPath, destFilePath, fsentry2.Size)
		} else {
			logger.Errorf("Unknown file type %q", fsentry2.Type)
		}
	}

	fmt.Printf("Upload took - %f Sec\n", ttaken.Seconds())
}
