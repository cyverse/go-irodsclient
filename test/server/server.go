package server

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/cockroachdb/errors"
	irods_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/docker/compose/v2/pkg/api"
	log "github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

type IRODSTestServer struct {
	Version           IRODSTestServerVersion
	DockerComposePath string
	Account           *types.IRODSAccount
	AddressResolver   func(address string) string
	terminateChan     chan bool
	terminateWait     *sync.WaitGroup
}

func getComposeFilePath(version IRODSTestServerVersion) (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.Errorf("failed to get current file path")
	}

	currentDir := filepath.Dir(currentFile)

	return fmt.Sprintf("%s/irods_%s/docker-compose.yml", currentDir, string(version)), nil
}

func getTestIRODSServerAccount() (*types.IRODSAccount, error) {
	account, err := types.CreateIRODSAccount(testServerHost, testServerPort, testServerAdminUser, testServerZone, types.AuthSchemeNative, testServerAdminPassword, testServerResource)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create irods account")
	}

	account.ClientServerNegotiation = false

	return account, nil
}

func getProductionIRODSServerAccount() (*types.IRODSAccount, error) {
	account, err := types.CreateIRODSAccount(productionServerHost, productionServerPort, productionServerAdminUser, productionServerZone, types.AuthSchemeNative, productionServerAdminPassword, productionServerResource)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create irods account")
	}

	account.ClientServerNegotiation = false

	return account, nil
}

func irodsServerAddressResolver(address string) string {
	return testServerHost
}

func GetTestIRODSVersions() []IRODSTestServerVersion {
	return Test_IRODS_Versions
}

func GetProductionIRODSVersions() []IRODSTestServerVersion {
	return Production_IRODS_Versions
}

func NewTestIRODSServer(version IRODSTestServerVersion) (*IRODSTestServer, error) {
	composePath, err := getComposeFilePath(version)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get compose file path")
	}

	account, err := getTestIRODSServerAccount()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get iRODS test server account")
	}

	return &IRODSTestServer{
		Version:           version,
		DockerComposePath: composePath,
		Account:           account,
		AddressResolver:   irodsServerAddressResolver,
		terminateChan:     make(chan bool),
		terminateWait:     &sync.WaitGroup{},
	}, nil
}

func NewProductionIRODSServer(version IRODSTestServerVersion) (*IRODSTestServer, error) {
	account, err := getProductionIRODSServerAccount()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get production iRODS server account")
	}

	return &IRODSTestServer{
		Version:           version,
		DockerComposePath: "",
		Account:           account,
		AddressResolver:   nil,
		terminateChan:     make(chan bool),
		terminateWait:     &sync.WaitGroup{},
	}, nil
}

func (server *IRODSTestServer) Start() error {
	logger := log.WithFields(log.Fields{})

	if len(server.DockerComposePath) == 0 {
		// Production server
		logger.Infof("Using production iRODS server %q", server.Version)
		return nil
	}

	logger.Infof("Starting iRODS test server %q", server.Version)

	comp, err := compose.NewDockerCompose(server.DockerComposePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create docker compose")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = comp.Up(ctx, compose.WithRecreate(api.RecreateForce), compose.Wait(true))
	if err != nil {
		return errors.Wrapf(err, "failed to start iRODS test server")
	}

	server.terminateWait.Add(1)

	logger.Infof("Started iRODS test server %q", server.Version)

	go func() {
		<-server.terminateChan
		logger.Infof("Stopping iRODS test server %q", server.Version)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err = comp.Down(ctx, compose.RemoveOrphans(true))
		if err != nil {
			logger.Error(errors.Wrapf(err, "failed to stop iRODS test server"))
		}

		server.terminateWait.Done()

		logger.Infof("Stopped iRODS test server %q", server.Version)
	}()

	return nil
}

func (server *IRODSTestServer) Stop() {
	if len(server.DockerComposePath) > 0 {
		server.terminateChan <- true
		server.terminateWait.Wait()
	}
}

func (server *IRODSTestServer) GetVersion() IRODSTestServerVersion {
	return server.Version
}

func (server *IRODSTestServer) GetAccount() *types.IRODSAccount {
	return server.Account
}

func (server *IRODSTestServer) GetAccountCopy() *types.IRODSAccount {
	accountCpy := *server.Account
	return &accountCpy
}

func (server *IRODSTestServer) GetApplicationName() string {
	return "go-irodsclient-test"
}

func (server *IRODSTestServer) GetConnectionConfig() *connection.IRODSConnectionConfig {
	return &connection.IRODSConnectionConfig{
		ApplicationName: server.GetApplicationName(),
	}
}

func (server *IRODSTestServer) GetFileSystemConfig() *irods_fs.FileSystemConfig {
	fsConfig := irods_fs.NewFileSystemConfig(server.GetApplicationName())
	fsConfig.AddressResolver = server.AddressResolver
	return fsConfig
}

func (server *IRODSTestServer) GetSessionConfig() *session.IRODSSessionConfig {
	fsConfig := server.GetFileSystemConfig()
	return fsConfig.ToIOSessionConfig()
}

func (server *IRODSTestServer) GetSession() (*session.IRODSSession, error) {
	account := server.GetAccountCopy()
	sessionConfig := server.GetSessionConfig()

	return session.NewIRODSSession(account, sessionConfig)
}

func (server *IRODSTestServer) GetFileSystem() (*irods_fs.FileSystem, error) {
	account := server.GetAccountCopy()
	fsConfig := server.GetFileSystemConfig()
	return irods_fs.NewFileSystem(account, fsConfig)
}

func (server *IRODSTestServer) GetHomeDir() string {
	account := server.GetAccountCopy()
	return account.GetHomeDirPath()
}
