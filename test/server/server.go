package server

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"

	irods_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/docker/compose/v2/pkg/api"
	log "github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"golang.org/x/xerrors"
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
		return "", fmt.Errorf("failed to get current file path")
	}

	currentDir := filepath.Dir(currentFile)

	return fmt.Sprintf("%s/irods_%s/docker-compose.yml", currentDir, string(version)), nil
}

func getIRODSServerAccount() (*types.IRODSAccount, error) {
	account, err := types.CreateIRODSAccount(testServerHost, testServerPort, testServerAdminUser, testServerZone, types.AuthSchemeNative, testServerAdminPassword, testServerResource)
	if err != nil {
		return nil, xerrors.Errorf("failed to create irods account: %w", err)
	}

	account.ClientServerNegotiation = false

	return account, nil
}

func irodsServerAddressResolver(address string) string {
	return testServerHost
}

func GetIRODSVersions() []IRODSTestServerVersion {
	return IRODS_Versions
}

func NewIRODSServer(version IRODSTestServerVersion) (*IRODSTestServer, error) {
	composePath, err := getComposeFilePath(version)
	if err != nil {
		return nil, xerrors.Errorf("failed to get compose file path: %w", err)
	}

	account, err := getIRODSServerAccount()
	if err != nil {
		return nil, xerrors.Errorf("failed to get iRODS test server account: %w", err)
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

func (server *IRODSTestServer) Start() error {
	logger := log.WithFields(log.Fields{
		"package":  "server",
		"stsruct":  "IRODSTestServer",
		"function": "Start",
	})

	logger.Infof("Starting iRODS test server %q", server.Version)

	comp, err := compose.NewDockerCompose(server.DockerComposePath)
	if err != nil {
		return xerrors.Errorf("failed to create docker compose: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = comp.Up(ctx, compose.WithRecreate(api.RecreateForce), compose.Wait(true))
	if err != nil {
		return xerrors.Errorf("failed to start iRODS test server: %w", err)
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
			logger.Error(xerrors.Errorf("failed to stop iRODS test server: %w", err))
		}

		server.terminateWait.Done()

		logger.Infof("Stopped iRODS test server %q", server.Version)
	}()

	return nil
}

func (server *IRODSTestServer) Stop() {
	server.terminateChan <- true
	server.terminateWait.Wait()
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
	return fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)
}
