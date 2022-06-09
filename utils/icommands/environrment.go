package icommands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/irods/types"
)

const (
	environmentDir  string = ".irods"
	passwordFile    string = ".irodsA"
	environmentFile string = "irods_environment.json"
)

// ICommandsEnvironmentManager is a struct that manages icommands environment files
type ICommandsEnvironmentManager struct {
	DirPath     string
	UID         int
	Password    string
	Environment *ICommandsEnvironment
	Session     *ICommandsEnvironment
}

// CreateIcommandsEnvironmentManager creates ICommandsEnvironmentManager
func CreateIcommandsEnvironmentManager(envDirPath string, uid int) (*ICommandsEnvironmentManager, error) {
	return &ICommandsEnvironmentManager{
		DirPath:     envDirPath,
		UID:         uid,
		Password:    "",
		Environment: &ICommandsEnvironment{},
		Session:     &ICommandsEnvironment{},
	}, nil
}

// CreateIcommandsEnvironmentManagerWithDefault creates ICommandsEnvironmentManager
func CreateIcommandsEnvironmentManagerWithDefault() (*ICommandsEnvironmentManager, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	irodsEnvDir := filepath.Join(homedir, environmentDir)

	uid := os.Getuid()

	return CreateIcommandsEnvironmentManager(irodsEnvDir, uid)
}

// CreateIcommandsEnvironmentManagerFromIRODSAccount creates ICommandsEnvironmentManager from IRODSAccount
func CreateIcommandsEnvironmentManagerFromIRODSAccount(envDirPath string, uid int, account *types.IRODSAccount) (*ICommandsEnvironmentManager, error) {
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

	environment := ICommandsEnvironment{
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
		environment.SSLCACertificateFile = account.SSLConfiguration.CACertificateFile
		environment.EncryptionKeySize = account.SSLConfiguration.EncryptionKeySize
		environment.EncryptionAlgorithm = account.SSLConfiguration.EncryptionAlgorithm
		environment.EncryptionSaltSize = account.SSLConfiguration.SaltSize
		environment.EncryptionNumHashRounds = account.SSLConfiguration.HashRounds
	}

	return &ICommandsEnvironmentManager{
		DirPath:     envDirPath,
		UID:         uid,
		Password:    account.Password,
		Environment: &environment,
		Session:     &ICommandsEnvironment{},
	}, nil
}

// CreateIcommandsEnvironmentManagerFromIRODSAccountWithDefault creates ICommandsEnvironmentManager
func CreateIcommandsEnvironmentManagerFromIRODSAccountWithDefault(account *types.IRODSAccount) (*ICommandsEnvironmentManager, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	irodsEnvDir := filepath.Join(homedir, environmentDir)

	uid := os.Getuid()

	return CreateIcommandsEnvironmentManagerFromIRODSAccount(irodsEnvDir, uid, account)
}

// Load loads from environment dir
func (mgr *ICommandsEnvironmentManager) Load(processID int) error {
	env, err := mgr.readEnvironment()
	if err != nil {
		return err
	}

	mgr.Environment = env

	session, err := mgr.readSession(processID)
	if err != nil {
		return err
	}

	mgr.Session = session

	password, err := mgr.readPassword()
	if err != nil {
		return err
	}

	mgr.Password = password
	return nil
}

// readEnvironment reads environment
func (mgr *ICommandsEnvironmentManager) readEnvironment() (*ICommandsEnvironment, error) {
	environmentFilePath := filepath.Join(mgr.DirPath, environmentFile)
	return CreateICommandsEnvironmentFromFile(environmentFilePath)
}

// readPassword decyphers password file (.irodsA)
func (mgr *ICommandsEnvironmentManager) readPassword() (string, error) {
	passwordFilePath := filepath.Join(mgr.DirPath, passwordFile)
	return DecodePasswordFile(passwordFilePath, mgr.UID)
}

// readSession reads session
func (mgr *ICommandsEnvironmentManager) readSession(processID int) (*ICommandsEnvironment, error) {
	sessionFilename := fmt.Sprintf("%s.%d", environmentFile, processID)
	sessionFilePath := filepath.Join(mgr.DirPath, sessionFilename)

	_, err := os.Stat(sessionFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &ICommandsEnvironment{}, nil
		} else {
			return nil, err
		}
	}

	return CreateICommandsEnvironmentFromFile(sessionFilePath)

}

func (mgr *ICommandsEnvironmentManager) ToIRODSAccount() (*types.IRODSAccount, error) {
	if mgr.Environment == nil {
		return nil, fmt.Errorf("environment is not set")
	}

	account := mgr.Environment.ToIRODSAccount()
	account.Password = mgr.Password
	return account, nil
}

// makeDir makes a directory for environment files
func (mgr *ICommandsEnvironmentManager) makeDir() error {
	st, err := os.Stat(mgr.DirPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(mgr.DirPath, 0700)
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	}

	if !st.IsDir() {
		return fmt.Errorf("path %s is not a directory", mgr.DirPath)
	}

	return nil
}

// Save saves to a dir
func (mgr *ICommandsEnvironmentManager) Save() error {
	err := mgr.makeDir()
	if err != nil {
		return err
	}

	if mgr.Environment == nil {
		return fmt.Errorf("environment is not set")
	}

	environmentFilePath := filepath.Join(mgr.DirPath, environmentFile)
	err = mgr.Environment.ToFile(environmentFilePath)
	if err != nil {
		return err
	}

	passwordFilePath := filepath.Join(mgr.DirPath, passwordFile)
	return EncodePasswordFile(passwordFilePath, mgr.Password, mgr.UID)
}

// SaveSession saves session to a dir
func (mgr *ICommandsEnvironmentManager) SaveSession(processID int) error {
	err := mgr.makeDir()
	if err != nil {
		return err
	}

	if mgr.Session == nil {
		return nil
	}

	sessionFilename := fmt.Sprintf("%s.%d", environmentFile, processID)
	sessionFilePath := filepath.Join(mgr.DirPath, sessionFilename)
	return mgr.Session.ToFile(sessionFilePath)
}
