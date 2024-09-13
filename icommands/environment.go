package icommands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

const (
	environmentDirDefault   string = "~/.irods"
	passwordFilenameDefault string = ".irodsA"
	environmentFilename     string = "irods_environment.json"
)

// ICommandsEnvironmentManager is a struct that manages icommands environment files
type ICommandsEnvironmentManager struct {
	HomeEnvironmentDirPath string
	EnvironmentDirPath     string
	EnvironmentFilename    string
	UID                    int
	Password               string
	PamToken               string
	Environment            *ICommandsEnvironment
	Session                *ICommandsEnvironment
}

// CreateIcommandsEnvironmentManager creates ICommandsEnvironmentManager
func CreateIcommandsEnvironmentManager() (*ICommandsEnvironmentManager, error) {
	uid := os.Getuid()

	envDirPath, err := util.ExpandHomeDir(environmentDirDefault)
	if err != nil {
		return nil, xerrors.Errorf("failed to expand home dir %q: %w", environmentDirDefault, err)
	}

	return &ICommandsEnvironmentManager{
		HomeEnvironmentDirPath: envDirPath,
		EnvironmentDirPath:     envDirPath,
		EnvironmentFilename:    environmentFilename,
		UID:                    uid,
		Password:               "",
		PamToken:               "",
		Environment:            &ICommandsEnvironment{},
		Session:                &ICommandsEnvironment{},
	}, nil
}

// CreateIcommandsEnvironmentManagerFromIRODSAccount creates ICommandsEnvironmentManager from IRODSAccount
func CreateIcommandsEnvironmentManagerFromIRODSAccount(account *types.IRODSAccount) (*ICommandsEnvironmentManager, error) {
	manager, err := CreateIcommandsEnvironmentManager()
	if err != nil {
		return nil, xerrors.Errorf("failed to create icommands environment manager: %w", err)
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

	sslVerifyServer := ""
	if account.SkipVerifyTLS {
		sslVerifyServer = "none"
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
		DefaultHashScheme:       account.DefaultHashScheme,
		SSLVerifyServer:         sslVerifyServer,
	}

	if account.SSLConfiguration != nil {
		manager.Environment.SSLCACertificateFile = account.SSLConfiguration.CACertificateFile
		manager.Environment.SSLCACertificatePath = account.SSLConfiguration.CACertificatePath
		manager.Environment.EncryptionKeySize = account.SSLConfiguration.EncryptionKeySize
		manager.Environment.EncryptionAlgorithm = account.SSLConfiguration.EncryptionAlgorithm
		manager.Environment.EncryptionSaltSize = account.SSLConfiguration.EncryptionSaltSize
		manager.Environment.EncryptionNumHashRounds = account.SSLConfiguration.EncryptionNumHashRounds
	}

	manager.Password = account.Password
	manager.PamToken = account.PamToken

	return manager, nil
}

func (manager *ICommandsEnvironmentManager) SetEnvironmentFilePath(envFilePath string) error {
	if len(envFilePath) > 0 {
		envFilePath, err := util.ExpandHomeDir(envFilePath)
		if err != nil {
			return xerrors.Errorf("failed to expand home dir %q: %w", envFilePath, err)
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
	if manager.Environment.AuthenticationFile != "" {
		return manager.Environment.AuthenticationFile
	}

	return filepath.Join(manager.HomeEnvironmentDirPath, passwordFilenameDefault)
}

// Load loads from environment file
func (manager *ICommandsEnvironmentManager) Load(processID int) error {
	logger := log.WithFields(log.Fields{
		"package":  "icommands",
		"struct":   "ICommandsEnvironmentManager",
		"function": "Load",
	})

	environmentFilePath := manager.GetEnvironmentFilePath()

	if util.ExistFile(environmentFilePath) {
		logger.Debugf("reading environment file %s", environmentFilePath)

		env, err := CreateICommandsEnvironmentFromFile(environmentFilePath)
		if err != nil {
			return xerrors.Errorf("failed to create icommands environment from file %q: %w", environmentFilePath, err)
		}

		manager.Environment = env
	}

	// read session
	sessionFilePath := manager.GetSessionFilePath(processID)
	if util.ExistFile(sessionFilePath) {
		logger.Debugf("reading environment session file %s", sessionFilePath)

		session, err := CreateICommandsEnvironmentFromFile(sessionFilePath)
		if err != nil {
			return xerrors.Errorf("failed to create icommands environment session from file %q: %w", sessionFilePath, err)
		}

		manager.Session = session
	}

	// read password (.irodsA)
	passwordFilePath := manager.GetPasswordFilePath()
	if util.ExistFile(passwordFilePath) {
		logger.Debugf("reading environment password file %s", passwordFilePath)

		password, err := DecodePasswordFile(passwordFilePath, manager.UID)
		if err != nil {
			logger.WithError(err).Debugf("failed to decode password file %q", passwordFilePath)
			//return xerrors.Errorf("failed to decode password file %q: %w", passwordFilePath, err)
			// continue
		} else {
			authScheme := types.GetAuthScheme(manager.Environment.AuthenticationScheme)
			if authScheme.IsPAM() {
				manager.Password = ""
				manager.PamToken = password
			} else {
				manager.Password = password
				manager.PamToken = ""
			}
		}
	}

	return nil
}

func (manager *ICommandsEnvironmentManager) ToIRODSAccount() (*types.IRODSAccount, error) {
	if manager.Environment == nil {
		return nil, xerrors.Errorf("environment is not set")
	}

	account := manager.Environment.ToIRODSAccount()
	account.Password = manager.Password
	account.PamToken = manager.PamToken

	return account, nil
}

// SaveEnvironment saves environment
func (manager *ICommandsEnvironmentManager) SaveEnvironment() error {
	if manager.Environment == nil {
		return xerrors.Errorf("environment is not set")
	}

	environmentFilePath := manager.GetEnvironmentFilePath()

	// make dir first if not exist
	dirpath := filepath.Dir(environmentFilePath)
	err := os.MkdirAll(dirpath, 0700)
	if err != nil {
		return xerrors.Errorf("failed to make a dir %q: %w", dirpath, err)
	}

	err = manager.Environment.ToFile(environmentFilePath)
	if err != nil {
		return xerrors.Errorf("failed to write environment to file %q: %w", environmentFilePath, err)
	}

	passwordFilePath := manager.GetPasswordFilePath()
	authScheme := types.GetAuthScheme(manager.Environment.AuthenticationScheme)

	password := manager.Password
	if authScheme.IsPAM() {
		password = manager.PamToken
	}

	err = EncodePasswordFile(passwordFilePath, password, manager.UID)
	if err != nil {
		return xerrors.Errorf("failed to encode password file %q: %w", passwordFilePath, err)
	}
	return nil
}

// SaveSession saves session to a dir
func (manager *ICommandsEnvironmentManager) SaveSession(processID int) error {
	if manager.Session == nil {
		return nil
	}

	sessionFilePath := manager.GetSessionFilePath(processID)

	// make dir first if not exist
	dirpath := filepath.Dir(sessionFilePath)
	err := os.MkdirAll(dirpath, 0700)
	if err != nil {
		return xerrors.Errorf("failed to make a dir %q: %w", dirpath, err)
	}

	err = manager.Session.ToFile(sessionFilePath)
	if err != nil {
		return xerrors.Errorf("failed to save to file %q: %w", sessionFilePath, err)
	}
	return nil
}
