package server

import (
	"fmt"
	"os/exec"
	"path"
	"runtime"

	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
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

func StartServer() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "StartServer",
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
		startErr := xerrors.Errorf("failed to start iRODS test server: %w", err)
		logger.Errorf("%+v", startErr)
		return startErr
	}

	cmd.Wait()

	return nil
}

func StopServer() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "StopServer",
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
		stopErr := xerrors.Errorf("failed to stop iRODS test server: %w", err)
		logger.Errorf("%+v", stopErr)
		return stopErr
	}

	cmd.Wait()
	// we don't check it's error because it always return exit code 1

	logger.Info("Successfully stopped iRODS test server")
	return nil
}

func GetLocalAccount() (*types.IRODSAccount, error) {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"function": "GetLocalAccount",
	})

	account, err := types.CreateIRODSAccount(testServerHost, testServerPort, testServerAdminUser, testServerZone, types.AuthSchemeNative, testServerAdminPassword, "")
	if err != nil {
		accountErr := xerrors.Errorf("failed to create irods account: %w", err)
		logger.Errorf("%+v", accountErr)
		return nil, accountErr
	}

	return account, nil
}
