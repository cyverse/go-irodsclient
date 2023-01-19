package icommands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

const (
	environmentDirDefault string = "~/.irods"
	passwordFilename      string = ".irodsA"
	environmentFilename   string = "irods_environment.json"
)

// ICommandsEnvironmentManager is a struct that manages icommands environment files
type ICommandsEnvironmentManager struct {
	HomeEnvironmentDirPath string
	EnvironmentDirPath     string
	EnvironmentFilename    string
	UID                    int
	Password               string
	IsPasswordPamToken     bool
	Environment            *ICommandsEnvironment
	Session                *ICommandsEnvironment
}

// CreateIcommandsEnvironmentManager creates ICommandsEnvironmentManager
func CreateIcommandsEnvironmentManager() (*ICommandsEnvironmentManager, error) {
	uid := os.Getuid()

	envDirPath, err := util.ExpandHomeDir(environmentDirDefault)
	if err != nil {
		return nil, err
	}

	return &ICommandsEnvironmentManager{
		HomeEnvironmentDirPath: envDirPath,
		EnvironmentDirPath:     envDirPath,
		EnvironmentFilename:    environmentFilename,
		UID:                    uid,
		Password:               "",
		IsPasswordPamToken:     false,
		Environment:            &ICommandsEnvironment{},
		Session:                &ICommandsEnvironment{},
	}, nil
}

// CreateIcommandsEnvironmentManagerFromIRODSAccount creates ICommandsEnvironmentManager from IRODSAccount
func CreateIcommandsEnvironmentManagerFromIRODSAccount(account *types.IRODSAccount) (*ICommandsEnvironmentManager, error) {
	manager, err := CreateIcommandsEnvironmentManager()
	if err != nil {
		return nil, err
	}

	csNegotiation := ""
	if account.ClientServerNegotiation {
		csNegotiation = "request_server_negotiation"
	}

	username := account.ClientUser
	if len(account.ProxyUser) > 0 {
		username = account.ProxyUser
	}

	zone := account.ClientZone
	if len(account.ProxyZone) > 0 {
		zone = account.ProxyZone
	}

	manager.Environment = &ICommandsEnvironment{
		AuthenticationScheme:    string(account.AuthenticationScheme),
		ClientServerNegotiation: csNegotiation,
		ClientServerPolicy:      string(account.CSNegotiationPolicy),
		Host:                    account.Host,
		Port:                    account.Port,
		Username:                username,
		Zone:                    zone,
		DefaultResource:         account.DefaultResource,
	}

	if account.SSLConfiguration != nil {
		manager.Environment.SSLCACertificateFile = account.SSLConfiguration.CACertificateFile
		manager.Environment.EncryptionKeySize = account.SSLConfiguration.EncryptionKeySize
		manager.Environment.EncryptionAlgorithm = account.SSLConfiguration.EncryptionAlgorithm
		manager.Environment.EncryptionSaltSize = account.SSLConfiguration.SaltSize
		manager.Environment.EncryptionNumHashRounds = account.SSLConfiguration.HashRounds
	}

	manager.Password = account.Password
	manager.IsPasswordPamToken = false

	return manager, nil
}

func (manager *ICommandsEnvironmentManager) SetEnvironmentFilePath(envFilePath string) error {
	if len(envFilePath) > 0 {
		envFilePath, err := util.ExpandHomeDir(envFilePath)
		if err != nil {
			return err
		}

		manager.EnvironmentDirPath = filepath.Dir(envFilePath)
		manager.EnvironmentFilename = filepath.Base(envFilePath)
	}
	return nil
}

func (manager *ICommandsEnvironmentManager) SetUID(uid int) {
	manager.UID = uid
}

// GetEnvironmentFilePath returns environment file (irods_environment.json) path
func (manager *ICommandsEnvironmentManager) GetEnvironmentFilePath() string {
	return filepath.Join(manager.EnvironmentDirPath, manager.EnvironmentFilename)
}

// GetSessionFilePath returns session file (irods_environment.json.<sessionid>) path
func (manager *ICommandsEnvironmentManager) GetSessionFilePath(processID int) string {
	if manager.EnvironmentDirPath == manager.HomeEnvironmentDirPath &&
		manager.EnvironmentFilename == environmentFilename {
		// default .irods dir
		sessionFilename := fmt.Sprintf("%s.%d", manager.EnvironmentFilename, processID)
		return filepath.Join(manager.EnvironmentDirPath, sessionFilename)
	}

	// custom irods config dir
	// creates .cwd file
	sessionFilename := fmt.Sprintf("%s.cwd", manager.EnvironmentFilename)
	return filepath.Join(manager.EnvironmentDirPath, sessionFilename)
}

// GetPasswordFilePath returns password file (.irodsA) path
func (manager *ICommandsEnvironmentManager) GetPasswordFilePath() string {
	return filepath.Join(manager.HomeEnvironmentDirPath, passwordFilename)
}

// Load loads from environment file
func (manager *ICommandsEnvironmentManager) Load(processID int) error {
	environmentFilePath := manager.GetEnvironmentFilePath()
	env, err := CreateICommandsEnvironmentFromFile(environmentFilePath)
	if err != nil {
		return err
	}

	manager.Environment = env

	// read session
	sessionFilePath := manager.GetSessionFilePath(processID)
	if util.ExistFile(sessionFilePath) {
		session, err := CreateICommandsEnvironmentFromFile(sessionFilePath)
		if err != nil {
			return err
		}

		manager.Session = session
	}

	// read password (.irodsA)
	passwordFilePath := manager.GetPasswordFilePath()
	password, err := DecodePasswordFile(passwordFilePath, manager.UID)
	if err != nil {
		return err
	}

	manager.Password = password
	manager.IsPasswordPamToken = false

	authScheme, _ := types.GetAuthScheme(manager.Environment.AuthenticationScheme)
	if authScheme == types.AuthSchemePAM {
		// if auth scheme is PAM auth, password read from .irodsA is pam token
		manager.IsPasswordPamToken = true
	}

	return nil
}

func (manager *ICommandsEnvironmentManager) ToIRODSAccount() (*types.IRODSAccount, error) {
	if manager.Environment == nil {
		return nil, fmt.Errorf("environment is not set")
	}

	account := manager.Environment.ToIRODSAccount()
	account.Password = manager.Password

	if manager.IsPasswordPamToken {
		account.AuthenticationScheme = types.AuthSchemeNative
	}

	return account, nil
}

// SaveEnvironment saves environment
func (manager *ICommandsEnvironmentManager) SaveEnvironment() error {
	if manager.Environment == nil {
		return fmt.Errorf("environment is not set")
	}

	environmentFilePath := manager.GetEnvironmentFilePath()

	// make dir first if not exist
	err := os.MkdirAll(filepath.Dir(environmentFilePath), 0700)
	if err != nil {
		return err
	}

	err = manager.Environment.ToFile(environmentFilePath)
	if err != nil {
		return err
	}

	passwordFilePath := filepath.Join(manager.HomeEnvironmentDirPath, passwordFilename)
	return EncodePasswordFile(passwordFilePath, manager.Password, manager.UID)
}

// SaveSession saves session to a dir
func (manager *ICommandsEnvironmentManager) SaveSession(processID int) error {
	if manager.Session == nil {
		return nil
	}

	sessionFilePath := manager.GetSessionFilePath(processID)

	// make dir first if not exist
	err := os.MkdirAll(filepath.Dir(sessionFilePath), 0700)
	if err != nil {
		return err
	}

	return manager.Session.ToFile(sessionFilePath)
}
