package config

import (
	"encoding/json"
	"os"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

// configuration default values
const (
	AuthenticationSchemeDefault    string = string(types.AuthSchemeNative)
	ClientServerNegotiationDefault string = string(types.CSNegotiationOff)
	ClientServerPolicyDefault      string = string(types.CSNegotiationPolicyRequestTCP)
	PortDefault                    int    = 1247
	HashSchemeDefault              string = types.HashSchemeDefault
	EncryptionAlgorithmDefault     string = "AES-256-CBC"
	EncryptionKeySizeDefault       int    = 32
	EncryptionSaltSizeDefault      int    = 8
	EncryptionNumHashRoundsDefault int    = 16
	SSLVerifyServerDefault         string = "hostname"
)

// Config stores irods config
type Config struct {
	AuthenticationScheme    string `json:"irods_authentication_scheme,omitempty" yaml:"irods_authentication_scheme,omitempty" envconfig:"IRODS_AUTHENTICATION_SCHEME"`
	AuthenticationFile      string `json:"irods_authentication_file,omitempty" yaml:"irods_authentication_file,omitempty" envconfig:"IRODS_AUTHENTICATION_FILE"`
	ClientServerNegotiation string `json:"irods_client_server_negotiation,omitempty" yaml:"irods_client_server_negotiation,omitempty" envconfig:"IRODS_CLIENT_SERVER_NEGOTIATION"`
	ClientServerPolicy      string `json:"irods_client_server_policy,omitempty" yaml:"irods_client_server_policy,omitempty" envconfig:"IRODS_CLIENT_SERVER_POLICY"`
	Host                    string `json:"irods_host,omitempty" yaml:"irods_host,omitempty" envconfig:"IRODS_HOST"`
	Port                    int    `json:"irods_port,omitempty" yaml:"irods_port,omitempty" envconfig:"IRODS_PORT"`
	ZoneName                string `json:"irods_zone_name,omitempty" yaml:"irods_zone_name,omitempty" envconfig:"IRODS_ZONE_NAME"`
	ClientZoneName          string `json:"irods_client_zone_name,omitempty" yaml:"irods_client_zone_name,omitempty" envconfig:"IRODS_CLIENT_ZONE_NAME"`
	Username                string `json:"irods_user_name,omitempty" yaml:"irods_user_name,omitempty" envconfig:"IRODS_USER_NAME"`
	ClientUsername          string `json:"irods_client_user_name,omitempty" yaml:"irods_client_user_name,omitempty" envconfig:"IRODS_CLIENT_USER_NAME"`
	DefaultResource         string `json:"irods_default_resource,omitempty" yaml:"irods_default_resource,omitempty" envconfig:"IRODS_DEFAULT_RESOURCE"`
	CurrentWorkingDir       string `json:"irods_cwd,omitempty" yaml:"irods_cwd,omitempty" envconfig:"IRODS_CWD"`
	Home                    string `json:"irods_home,omitempty" yaml:"irods_home,omitempty" envconfig:"IRODS_HOME"`
	DefaultHashScheme       string `json:"irods_default_hash_scheme,omitempty" yaml:"irods_default_hash_scheme,omitempty" envconfig:"IRODS_DEFAULT_HASH_SCHEME"`
	MatchHashPolicy         string `json:"irods_match_hash_policy,omitempty" yaml:"irods_match_hash_policy,omitempty" envconfig:"IRODS_MATCH_HASH_POLICY"`
	Debug                   bool   `json:"irods_debug,omitempty" yaml:"irods_debug,omitempty" envconfig:"IRODS_DEBUG"`
	LogLevel                int    `json:"irods_log_level,omitempty" yaml:"irods_log_level,omitempty" envconfig:"IRODS_LOG_LEVEL"`
	EncryptionAlgorithm     string `json:"irods_encryption_algorithm,omitempty" yaml:"irods_encryption_algorithm,omitempty" envconfig:"IRODS_ENCRYPTION_ALGORITHM"`
	EncryptionKeySize       int    `json:"irods_encryption_key_size,omitempty" yaml:"irods_encryption_key_size,omitempty" envconfig:"IRODS_ENCRYPTION_KEY_SIZE"`
	EncryptionSaltSize      int    `json:"irods_encryption_salt_size,omitempty" yaml:"irods_encryption_salt_size,omitempty" envconfig:"IRODS_ENCRYPTION_SALT_SIZE"`
	EncryptionNumHashRounds int    `json:"irods_encryption_num_hash_rounds,omitempty" yaml:"irods_encryption_num_hash_rounds,omitempty" envconfig:"IRODS_ENCRYPTION_NUM_HASH_ROUNDS"`
	SSLCACertificateFile    string `json:"irods_ssl_ca_certificate_file,omitempty" yaml:"irods_ssl_ca_certificate_file,omitempty" envconfig:"IRODS_SSL_CA_CERTIFICATE_FILE"`
	SSLCACertificatePath    string `json:"irods_ssl_ca_certificate_path,omitempty" yaml:"irods_ssl_ca_certificate_path,omitempty" envconfig:"IRODS_SSL_CA_CERTIFICATE_PATH"`
	SSLVerifyServer         string `json:"irods_ssl_verify_server,omitempty" yaml:"irods_ssl_verify_server,omitempty" envconfig:"IRODS_SSL_VERIFY_SERVER"`
	SSLCertificateChainFile string `json:"irods_ssl_certificate_chain_file,omitempty" yaml:"irods_ssl_certificate_chain_file,omitempty" envconfig:"IRODS_SSL_CERTIFICATE_CHAIN_FILE"`
	SSLCertificateKeyFile   string `json:"irods_ssl_certificate_key_file,omitempty" yaml:"irods_ssl_certificate_key_file,omitempty" envconfig:"IRODS_SSL_CERTIFICATE_KEY_FILE"`
	SSLDHParamsFile         string `json:"irods_ssl_dh_params_file,omitempty" yaml:"irods_ssl_dh_params_file,omitempty" envconfig:"IRODS_SSL_DH_PARAMS_FILE"`

	// go-irodsclient only
	Password      string `json:"irods_user_password,omitempty" yaml:"irods_user_password,omitempty" envconfig:"IRODS_USER_PASSWORD"`
	Ticket        string `json:"irods_ticket,omitempty" yaml:"irods_ticket,omitempty" envconfig:"IRODS_TICKET"`
	PAMToken      string `json:"irods_pam_token,omitempty" yaml:"irods_pam_token,omitempty" envconfig:"IRODS_PAM_TOKEN"`
	PAMTTL        int    `json:"irods_pam_ttl,omitempty" yaml:"irods_pam_ttl,omitempty" envconfig:"IRODS_PAM_TTL"`
	SSLServerName string `json:"irods_ssl_server_name,omitempty" yaml:"irods_ssl_server_name,omitempty" envconfig:"IRODS_SSL_SERVER_NAME"`

	// not used
	GSIServerDN string `json:"irods_gsi_server_dn,omitempty" yaml:"irods_gsi_server_dn,omitempty" envconfig:"IRODS_GSI_SERVER_DN"`
}

func GetDefaultConfig() *Config {
	return &Config{
		AuthenticationScheme:    AuthenticationSchemeDefault,
		AuthenticationFile:      GetDefaultPasswordFilePath(),
		ClientServerNegotiation: ClientServerNegotiationDefault,
		ClientServerPolicy:      ClientServerPolicyDefault,
		Port:                    PortDefault,
		DefaultHashScheme:       HashSchemeDefault,
		Debug:                   false,
		LogLevel:                0,
		EncryptionAlgorithm:     EncryptionAlgorithmDefault,
		EncryptionKeySize:       EncryptionKeySizeDefault,
		EncryptionSaltSize:      EncryptionSaltSizeDefault,
		EncryptionNumHashRounds: EncryptionNumHashRoundsDefault,
		SSLCACertificateFile:    "",
		SSLCACertificatePath:    "",
		SSLVerifyServer:         SSLVerifyServerDefault,
	}
}

// NewConfigFromYamlFile creates Config from YAML
func NewConfigFromYamlFile(yamlPath string) (*Config, error) {
	config := GetDefaultConfig()

	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read YAML file %q: %w", yamlPath, err)
	}

	err = yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal YAML file %q to config: %w", yamlPath, err)
	}

	return config, nil
}

// NewConfigFromJsonFile creates Config from JSON
func NewConfigFromJsonFile(jsonPath string) (*Config, error) {
	config := GetDefaultConfig()

	jsonBytes, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read YAML file %q: %w", jsonPath, err)
	}

	err = json.Unmarshal(jsonBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal JSON file %q to config: %w", jsonPath, err)
	}

	return config, nil
}

// NewConfigFromJson creates Config from JSON
func NewConfigFromJson(jsonBytes []byte) (*Config, error) {
	config := GetDefaultConfig()

	err := json.Unmarshal(jsonBytes, config)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal JSON to Config: %w", err)
	}

	return config, nil
}

// NewConfigFromEnv creates Config from Environmental variables
func NewConfigFromEnv() (*Config, error) {
	config := GetDefaultConfig()

	err := envconfig.Process("", config)
	if err != nil {
		return nil, xerrors.Errorf("failed to read config from environmental variables: %w", err)
	}

	return config, nil
}

// OverwriteConfigFromEnv overwrites Config from Environmental variables
func OverwriteConfigFromEnv(config *Config) (*Config, error) {
	if config == nil {
		config = GetDefaultConfig()
	}

	err := envconfig.Process("", config)
	if err != nil {
		return nil, xerrors.Errorf("failed to read config from environmental variables: %w", err)
	}

	return config, nil
}

// GetDefaultIRODSConfigPath returns default config path
func GetDefaultIRODSConfigPath() string {
	irodsConfigPath, err := util.ExpandHomeDir("~/.irods")
	if err != nil {
		return ""
	}

	return irodsConfigPath
}

// ToIRODSAccount creates IRODSAccount
func (cfg *Config) ToIRODSAccount() *types.IRODSAccount {
	authScheme := types.GetAuthScheme(cfg.AuthenticationScheme)

	negotiationPolicy, _ := types.GetCSNegotiationPolicyRequest(cfg.ClientServerPolicy)
	negotiation, _ := types.GetCSNegotiation(cfg.ClientServerNegotiation)

	verifyServer, _ := types.GetSSLVerifyServer(cfg.SSLVerifyServer)

	account := &types.IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: negotiation.IsNegotiationRequired(),
		CSNegotiationPolicy:     negotiationPolicy,
		Host:                    cfg.Host,
		Port:                    cfg.Port,
		ClientUser:              cfg.ClientUsername,
		ClientZone:              cfg.ClientZoneName,
		ProxyUser:               cfg.Username,
		ProxyZone:               cfg.ZoneName,
		Password:                cfg.Password,
		DefaultResource:         cfg.DefaultResource,
		DefaultHashScheme:       cfg.DefaultHashScheme,
		PamTTL:                  cfg.PAMTTL,
		PamToken:                cfg.PAMToken,
		SSLConfiguration: &types.IRODSSSLConfig{
			CACertificateFile:       cfg.SSLCACertificateFile,
			CACertificatePath:       cfg.SSLCACertificatePath,
			EncryptionKeySize:       cfg.EncryptionKeySize,
			EncryptionAlgorithm:     cfg.EncryptionAlgorithm,
			EncryptionSaltSize:      cfg.EncryptionSaltSize,
			EncryptionNumHashRounds: cfg.EncryptionNumHashRounds,
			VerifyServer:            verifyServer,
			DHParamsFile:            cfg.SSLDHParamsFile,
			ServerName:              cfg.SSLServerName,
		},
	}

	account.FixAuthConfiguration()

	return account
}

// ClearICommandsIncompatibleFields clears all icommands-incompatible fields
func (cfg *Config) ClearICommandsIncompatibleFields() *Config {
	cfg2 := *cfg

	cfg2.Password = ""
	cfg2.Ticket = ""
	cfg2.PAMToken = ""
	cfg2.PAMTTL = 0
	cfg2.SSLServerName = ""

	return &cfg2
}

// ToJSON converts to JSON bytes
func (cfg *Config) ToJSON() ([]byte, error) {
	jsonBytes, err := json.MarshalIndent(cfg, "", "    ")
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal configuration to json: %w", err)
	}

	return jsonBytes, nil
}

// ToFile saves to a file
func (cfg *Config) ToFile(envPath string) error {
	jsonByte, err := cfg.ToJSON()
	if err != nil {
		return err
	}

	err = os.WriteFile(envPath, jsonByte, 0664)
	if err != nil {
		return xerrors.Errorf("failed to write to file %q: %w", envPath, err)
	}
	return nil
}
