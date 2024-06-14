package icommands

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// ICommandsEnvironment stores irods environment data (config file)
type ICommandsEnvironment struct {
	AuthenticationFile      string `json:"irods_authentication_file,omitempty"`
	AuthenticationScheme    string `json:"irods_authentication_scheme,omitempty"`
	ClientServerNegotiation string `json:"irods_client_server_negotiation,omitempty"`
	ClientServerPolicy      string `json:"irods_client_server_policy,omitempty"`
	ControlPlanePort        int    `json:"irods_control_plane_port,omitempty"`
	ControlPlaneKey         string `json:"irods_control_plane_key,omitempty"`
	CurrentWorkingDir       string `json:"irods_cwd,omitempty"`
	Debug                   int    `json:"irods_debug,omitempty"`
	DefaultHashScheme       string `json:"irods_default_hash_scheme,omitempty"`
	Host                    string `json:"irods_host,omitempty"`
	Port                    int    `json:"irods_port,omitempty"`
	Username                string `json:"irods_user_name,omitempty"`
	Zone                    string `json:"irods_zone_name,omitempty"`
	DefaultResource         string `json:"irods_default_resource,omitempty"`
	EncryptionAlgorithm     string `json:"irods_encryption_algorithm,omitempty"`
	EncryptionKeySize       int    `json:"irods_encryption_key_size,omitempty"`
	EncryptionNumHashRounds int    `json:"irods_encryption_num_hash_rounds,omitempty"`
	EncryptionSaltSize      int    `json:"irods_encryption_salt_size,omitempty"`
	GSIServerDN             string `json:"irods_gsi_server_dn,omitempty"`
	Home                    string `json:"irods_home,omitempty"`
	LogLevel                int    `json:"irods_log_level,omitempty"`
	MatchHashPolicy         string `json:"irods_match_hash_policy,omitempty"`
	PluginsHome             string `json:"irods_plugins_home,omitempty"`
	SSLCACertificateFile    string `json:"irods_ssl_ca_certificate_file,omitempty"`
	SSLCACertificatePath    string `json:"irods_ssl_ca_certificate_path,omitempty"`
	SSLCertificateChainFile string `json:"irods_ssl_certificate_chain_file,omitempty"`
	SSLCertificateKeyFile   string `json:"irods_ssl_certificate_key_file,omitempty"`
	SSLDHParamsFile         string `json:"irods_ssl_dh_params_file,omitempty"`
	SSLVerifyServer         string `json:"irods_ssl_verify_server,omitempty"`
	XMessageHost            string `json:"irods_xmsg_host,omitempty"`
	XMessagePort            int    `json:"irods_xmsg_port,omitempty"`
}

// CreateICommandsEnvironmentFromFile creates ICommandsEnvironment from a file
func CreateICommandsEnvironmentFromFile(envPath string) (*ICommandsEnvironment, error) {
	data, err := os.ReadFile(envPath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read from file %s: %w", envPath, err)
	}

	return CreateICommandsEnvironmentFromJSON(data)
}

func getDefaultICommandsEnvironment() *ICommandsEnvironment {
	return &ICommandsEnvironment{
		AuthenticationFile:      "",
		Port:                    1247,
		AuthenticationScheme:    "native",
		ClientServerNegotiation: "", // don't perform negotiation
		ClientServerPolicy:      string(types.CSNegotiationRequireTCP),
		DefaultHashScheme:       "SHA256",
		EncryptionKeySize:       32,
		EncryptionAlgorithm:     "AES-256-CBC",
		EncryptionSaltSize:      8,
		EncryptionNumHashRounds: 16,
		SSLVerifyServer:         "hostname",
	}
}

// CreateICommandsEnvironmentFromJSON creates ICommandsEnvironment from JSON
func CreateICommandsEnvironmentFromJSON(jsonBytes []byte) (*ICommandsEnvironment, error) {
	environment := getDefaultICommandsEnvironment()
	err := json.Unmarshal(jsonBytes, &environment)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal json to icommands environment: %w", err)
	}

	return environment, nil
}

// ToIRODSAccount creates IRODSAccount
func (env *ICommandsEnvironment) ToIRODSAccount() *types.IRODSAccount {
	authScheme := types.GetAuthScheme(env.AuthenticationScheme)

	negotiationRequired := false
	negotiationPolicy, _ := types.GetCSNegotiationRequire(env.ClientServerPolicy)

	if strings.ToLower(env.ClientServerNegotiation) == "request_server_negotiation" {
		negotiationRequired = true
	}

	skipVerifyTLS := false
	if strings.ToLower(env.SSLVerifyServer) == "none" {
		skipVerifyTLS = true
	}

	account := &types.IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: negotiationRequired,
		CSNegotiationPolicy:     negotiationPolicy,
		Host:                    env.Host,
		Port:                    env.Port,
		ClientUser:              env.Username,
		ClientZone:              env.Zone,
		ProxyUser:               env.Username,
		ProxyZone:               env.Zone,
		Password:                "",
		DefaultResource:         env.DefaultResource,
		DefaultHashScheme:       env.DefaultHashScheme,
		PamTTL:                  types.PamTTLDefault,
		PamToken:                "",
		SSLConfiguration: &types.IRODSSSLConfig{
			CACertificateFile:   env.SSLCACertificateFile,
			CACertificatePath:   env.SSLCACertificatePath,
			EncryptionKeySize:   env.EncryptionKeySize,
			EncryptionAlgorithm: env.EncryptionAlgorithm,
			SaltSize:            env.EncryptionSaltSize,
			HashRounds:          env.EncryptionNumHashRounds,
		},
		ServerNameTLS: "",
		SkipVerifyTLS: skipVerifyTLS,
	}

	account.FixAuthConfiguration()

	return account
}

// ToJSON converts to JSON bytes
func (env *ICommandsEnvironment) ToJSON() ([]byte, error) {
	jsonBytes, err := json.MarshalIndent(env, "", "    ")
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal icommands environment to json: %w", err)
	}

	return jsonBytes, nil
}

// ToFile saves to a file
func (env *ICommandsEnvironment) ToFile(envPath string) error {
	jsonByte, err := env.ToJSON()
	if err != nil {
		return xerrors.Errorf("failed to convert icommands environment to json: %w", err)
	}

	err = os.WriteFile(envPath, jsonByte, 0664)
	if err != nil {
		return xerrors.Errorf("failed to write to file %s: %w", envPath, err)
	}
	return nil
}
