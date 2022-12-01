package server

import (
	"fmt"
	"os/exec"
	"path"
	"runtime"

	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

const (
	// must have the same information as in `docker-compose.yml` and `config.inc`
	testServerContainer     string = "irods_test-irods-1"
	testServerHost          string = "localhost"
	testServerPort          int    = 1247
	testServerAdminUser     string = "rods"
	testServerAdminPassword string = "test_rods_password"
	testServerZone          string = "cyverse"
)

func startServerExec() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "startServerExec",
	})

	logger.Info("Running iRODS test server")
	_, callerPath, _, _ := runtime.Caller(0)
	serverDir := path.Dir(callerPath)
	scriptPath := fmt.Sprintf("%s/%s", serverDir, "start.sh")

	logger.Debugf("Executing %s", scriptPath)
	cmd := exec.Command(scriptPath)
	cmd.Dir = serverDir

	err := cmd.Start()
	if err != nil {
		logger.WithError(err).Errorf("failed to start iRODS test server")
		return err
	}

	cmd.Wait()

	return nil
}

func stopServerExec() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "stopServerExec",
	})

	logger.Info("Stopping iRODS test server")
	_, callerPath, _, _ := runtime.Caller(0)
	serverDir := path.Dir(callerPath)
	scriptPath := fmt.Sprintf("%s/%s", serverDir, "stop.sh")

	logger.Debugf("Executing %s", scriptPath)
	cmd := exec.Command(scriptPath)
	cmd.Dir = serverDir

	err := cmd.Start()
	if err != nil {
		logger.WithError(err).Errorf("failed to stop iRODS test server")
		return err
	}

	cmd.Wait()
	// we don't check it's error because it always return exit code 1

	logger.Info("Successfully stopped iRODS test server")
	return nil
}

func StartServer() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "StartServer",
	})

	err := startServerExec()
	if err != nil {
		logger.WithError(err).Error("failed to start iRODS test server")
		return err
	}

	return nil
}

func GetLocalAccount() (*types.IRODSAccount, error) {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "GetLocalAccount",
	})

	account, err := types.CreateIRODSAccount(testServerHost, testServerPort, testServerAdminUser, testServerZone, types.AuthSchemeNative, testServerAdminPassword, "")
	if err != nil {
		logger.WithError(err).Error("failed to create an iRODS Account")
		return nil, err
	}

	return account, nil
}

func StopServer() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "StopServer",
	})

	err := stopServerExec()
	if err != nil {
		logger.WithError(err).Error("failed to stop iRODS test server")
		return err
	}

	return nil

}
