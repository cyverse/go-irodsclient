package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

const (
	environmentDirDefault      string = "~/.irods"
	passwordFilenameDefault    string = ".irodsA"
	environmentFilenameDefault string = "irods_environment.json"
)

// GetDefaultEnvironmentDirPath returns default environment dir path
func GetDefaultEnvironmentDirPath() string {
	environmentDirPath, err := util.ExpandHomeDir(environmentDirDefault)
	if err != nil {
		environmentDirPath = environmentDirDefault
	}
	return environmentDirPath
}

// GetDefaultEnvironmentFilePath returns default environment file path
func GetDefaultEnvironmentFilePath() string {
	return filepath.Join(GetDefaultEnvironmentDirPath(), environmentFilenameDefault)
}

// GetDefaultPasswordFilePath returns default password file path
func GetDefaultPasswordFilePath() string {
	return filepath.Join(GetDefaultEnvironmentDirPath(), passwordFilenameDefault)
}

// ICommandsEnvironmentManager is a struct that manages icommands environment files
type ICommandsEnvironmentManager struct {
	EnvironmentDirPath  string
	EnvironmentFilePath string
	SessionFilePath     string
	PasswordFilePath    string

	UID  int
	PPID int

	Environment *Config
	Session     *Config
}

// NewICommandsEnvironmentManager creates ICommandsEnvironmentManager
func NewICommandsEnvironmentManager() (*ICommandsEnvironmentManager, error) {
	environmentDirPath := GetDefaultEnvironmentDirPath()
	environmentFilePath := GetDefaultEnvironmentFilePath()
	ppid := os.Getppid()
	sessionFilename := fmt.Sprintf("%s.%d", environmentFilenameDefault, ppid)
	sessionFilePath := filepath.Join(environmentDirPath, sessionFilename)
	passwordFilePath := GetDefaultPasswordFilePath()

	return &ICommandsEnvironmentManager{
		EnvironmentDirPath:  environmentDirPath,
		EnvironmentFilePath: environmentFilePath,
		SessionFilePath:     sessionFilePath,
		PasswordFilePath:    passwordFilePath,

		UID:  os.Getuid(),
		PPID: ppid,

		Environment: GetDefaultConfig(),
		Session:     &Config{},
	}, nil
}

// SetPPID sets ppid of environment, used to obfuscate password
func (manager *ICommandsEnvironmentManager) SetPPID(ppid int) {
	manager.PPID = ppid

	sessionFilename := fmt.Sprintf("%s.%d", environmentFilenameDefault, manager.PPID)
	sessionFilePath := filepath.Join(manager.EnvironmentDirPath, sessionFilename)

	manager.SessionFilePath = sessionFilePath
}

// FixAuthConfiguration fixes auth configuration
func (manager *ICommandsEnvironmentManager) FixAuthConfiguration() {
	manager.Environment.FixAuthConfiguration()
}

// FromIRODSAccount loads from IRODSAccount
func (manager *ICommandsEnvironmentManager) FromIRODSAccount(account *types.IRODSAccount) {
	account.FixAuthConfiguration()

	if manager.Environment == nil {
		manager.Environment = &Config{}
	}

	if manager.Session == nil {
		manager.Session = &Config{}
	}

	manager.Environment.AuthenticationScheme = string(account.AuthenticationScheme)
	manager.Environment.AuthenticationFile = manager.PasswordFilePath
	manager.Environment.ClientServerPolicy = string(account.CSNegotiationPolicy)
	manager.Environment.Host = account.Host
	manager.Environment.Port = account.Port

	if account.ClientServerNegotiation {
		manager.Environment.ClientServerNegotiation = string(types.CSNegotiationRequestServerNegotiation)
	}

	manager.Environment.Username = account.ProxyUser
	manager.Environment.ClientUsername = account.ClientUser

	manager.Environment.ZoneName = account.ProxyZone
	manager.Environment.ClientZoneName = account.ClientZone

	manager.Environment.Password = account.Password
	manager.Environment.Ticket = account.Ticket
	manager.Environment.PAMToken = account.PAMToken
	manager.Environment.PAMTTL = account.PamTTL

	manager.Environment.DefaultResource = account.DefaultResource
	manager.Environment.DefaultHashScheme = account.DefaultHashScheme

	if account.SSLConfiguration != nil {
		manager.Environment.SSLCACertificateFile = account.SSLConfiguration.CACertificateFile
		manager.Environment.SSLCACertificatePath = account.SSLConfiguration.CACertificatePath
		manager.Environment.EncryptionKeySize = account.SSLConfiguration.EncryptionKeySize
		manager.Environment.EncryptionAlgorithm = account.SSLConfiguration.EncryptionAlgorithm
		manager.Environment.EncryptionSaltSize = account.SSLConfiguration.EncryptionSaltSize
		manager.Environment.EncryptionNumHashRounds = account.SSLConfiguration.EncryptionNumHashRounds
		manager.Environment.SSLVerifyServer = string(account.SSLConfiguration.VerifyServer)
		manager.Environment.SSLDHParamsFile = account.SSLConfiguration.DHParamsFile
		manager.Environment.SSLServerName = account.SSLConfiguration.ServerName
	}

	manager.FixAuthConfiguration()
}

// SetEnvironmentFilePath sets environment file path
func (manager *ICommandsEnvironmentManager) SetEnvironmentFilePath(envFilePath string) error {
	envFilePath, err := util.ExpandHomeDir(envFilePath)
	if err != nil {
		return xerrors.Errorf("failed to expand home dir %q: %w", envFilePath, err)
	}

	manager.EnvironmentDirPath = filepath.Dir(envFilePath)
	manager.EnvironmentFilePath = envFilePath
	manager.SessionFilePath = fmt.Sprintf("%s.%d", manager.EnvironmentFilePath, manager.PPID)
	manager.PasswordFilePath = filepath.Join(manager.EnvironmentDirPath, passwordFilenameDefault)

	manager.Environment.AuthenticationFile = manager.PasswordFilePath
	return nil
}

// SetEnvironmentDirPath sets environment dir path
func (manager *ICommandsEnvironmentManager) SetEnvironmentDirPath(envDirPath string) error {
	envDirPath, err := util.ExpandHomeDir(envDirPath)
	if err != nil {
		return xerrors.Errorf("failed to expand home dir %q: %w", envDirPath, err)
	}

	manager.EnvironmentDirPath = envDirPath
	manager.EnvironmentFilePath = filepath.Join(envDirPath, environmentFilenameDefault)
	manager.SessionFilePath = fmt.Sprintf("%s.%d", manager.EnvironmentFilePath, manager.PPID)
	manager.PasswordFilePath = filepath.Join(manager.EnvironmentDirPath, passwordFilenameDefault)

	manager.Environment.AuthenticationFile = manager.PasswordFilePath
	return nil
}

// Load loads from environment file
func (manager *ICommandsEnvironmentManager) Load() error {
	logger := log.WithFields(log.Fields{
		"package":  "icommands",
		"struct":   "ICommandsEnvironmentManager",
		"function": "Load",
	})

	if len(manager.EnvironmentFilePath) > 0 {
		if util.ExistFile(manager.EnvironmentFilePath) {
			logger.Debugf("reading icommands configuration file %q", manager.EnvironmentFilePath)

			cfg, err := NewConfigFromFile(GetDefaultConfig(), manager.EnvironmentFilePath)
			if err != nil {
				return xerrors.Errorf("failed to create icommands configuration from file %q: %w", manager.EnvironmentFilePath, err)
			}

			manager.Environment = cfg

			if len(manager.Environment.AuthenticationFile) > 0 {
				manager.PasswordFilePath = manager.Environment.AuthenticationFile
			}
		}
	}

	// read session
	if len(manager.SessionFilePath) > 0 {
		if util.ExistFile(manager.SessionFilePath) {
			logger.Debugf("reading icommands session file %q", manager.SessionFilePath)

			cfg, err := NewConfigFromJSONFile(nil, manager.SessionFilePath)
			if err != nil {
				return xerrors.Errorf("failed to create icommands session from file %q: %w", manager.SessionFilePath, err)
			}

			manager.Session = cfg
		}
	}

	// read password (.irodsA)
	if len(manager.PasswordFilePath) > 0 {
		if util.ExistFile(manager.PasswordFilePath) {
			logger.Debugf("reading icommands password file %q", manager.PasswordFilePath)

			obfuscator := NewPasswordObfuscator()
			obfuscator.SetUID(manager.UID)
			passwordBytes, err := obfuscator.DecodeFile(manager.PasswordFilePath)
			if err != nil {
				logger.WithError(err).Warnf("failed to decode password file %q", manager.PasswordFilePath)
				// continue
			} else {
				authScheme := types.GetAuthScheme(manager.Environment.AuthenticationScheme)
				if authScheme.IsPAM() {
					manager.Environment.Password = ""
					manager.Environment.PAMToken = string(passwordBytes)
				} else {
					manager.Environment.Password = string(passwordBytes)
					manager.Environment.PAMToken = ""
				}
			}
		}
	}

	manager.FixAuthConfiguration()

	return nil
}

// GetSessionConfig returns session config that is merged with environment
func (manager *ICommandsEnvironmentManager) GetSessionConfig() (*Config, error) {
	if manager.Environment == nil && manager.Session == nil {
		return nil, xerrors.Errorf("environment is not set")
	}

	manager.FixAuthConfiguration()

	envJSONBytes, err := manager.Environment.ToJSON()
	if err != nil {
		return nil, xerrors.Errorf("failed to get json from environment")
	}

	envMap := map[string]interface{}{}
	err = json.Unmarshal(envJSONBytes, &envMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	sessionJSONBytes, err := manager.Session.ToJSON()
	if err != nil {
		return nil, xerrors.Errorf("failed to get json from session")
	}

	sessionMap := map[string]interface{}{}
	err = json.Unmarshal(sessionJSONBytes, &sessionMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal JSON to map: %w", err)
	}

	// merge
	for k, v := range sessionMap {
		// overwrite
		envMap[k] = v
	}

	newEnvBytes, err := json.Marshal(envMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal map to JSON: %w", err)
	}

	return NewConfigFromJSON(nil, newEnvBytes)
}

// ToIRODSAccount exports to IRODSAccount
func (manager *ICommandsEnvironmentManager) ToIRODSAccount() (*types.IRODSAccount, error) {
	if manager.Environment == nil {
		return nil, xerrors.Errorf("environment is not set")
	}

	manager.FixAuthConfiguration()

	return manager.Environment.ToIRODSAccount(), nil
}

// SaveEnvironment saves environment
func (manager *ICommandsEnvironmentManager) SaveEnvironment() error {
	if manager.Environment == nil {
		return xerrors.Errorf("environment is not set")
	}

	manager.FixAuthConfiguration()

	if len(manager.EnvironmentFilePath) > 0 {
		// make dir first if not exist
		dirpath := filepath.Dir(manager.EnvironmentFilePath)
		err := os.MkdirAll(dirpath, 0700)
		if err != nil {
			return xerrors.Errorf("failed to make a dir %q: %w", dirpath, err)
		}

		newEnv := manager.Environment.ClearICommandsIncompatibleFields()
		err = newEnv.ToFile(manager.EnvironmentFilePath)
		if err != nil {
			return xerrors.Errorf("failed to write icommands configuration to file %q: %w", manager.EnvironmentFilePath, err)
		}
	}

	if len(manager.PasswordFilePath) > 0 {
		authScheme := types.GetAuthScheme(manager.Environment.AuthenticationScheme)

		password := manager.Environment.Password
		if authScheme.IsPAM() {
			password = manager.Environment.PAMToken
		}

		obfuscator := NewPasswordObfuscator()
		obfuscator.SetUID(manager.UID)

		err := obfuscator.EncodeToFile(manager.PasswordFilePath, []byte(password))
		if err != nil {
			return xerrors.Errorf("failed to encode password to file %q: %w", manager.PasswordFilePath, err)
		}
	}

	return nil
}

// SaveSession saves session to a dir
func (manager *ICommandsEnvironmentManager) SaveSession() error {
	if manager.Session == nil {
		return nil
	}

	manager.FixAuthConfiguration()

	if len(manager.SessionFilePath) > 0 {
		// make dir first if not exist
		dirpath := filepath.Dir(manager.SessionFilePath)
		err := os.MkdirAll(dirpath, 0700)
		if err != nil {
			return xerrors.Errorf("failed to make a dir %q: %w", dirpath, err)
		}

		newSession := manager.Session.ClearICommandsIncompatibleFields()
		err = newSession.ToFile(manager.SessionFilePath)
		if err != nil {
			return xerrors.Errorf("failed to save to file %q: %w", manager.SessionFilePath, err)
		}
	}

	return nil
}
