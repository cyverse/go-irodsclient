package icommands

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/cyverse/go-irodsclient/irods/types"
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
	data, err := ioutil.ReadFile(envPath)
	if err != nil {
		return nil, err
	}

	return CreateICommandsEnvironmentFromJSON(data)
}

// CreateICommandsEnvironmentFromJSON creates ICommandsEnvironment from JSON
func CreateICommandsEnvironmentFromJSON(jsonBytes []byte) (*ICommandsEnvironment, error) {
	var environment ICommandsEnvironment
	err := json.Unmarshal(jsonBytes, &environment)
	if err != nil {
		return nil, fmt.Errorf("JSON Unmarshal Error - %v", err)
	}

	return &environment, nil
}

// ToIRODSAccount creates IRODSAccount
func (env *ICommandsEnvironment) ToIRODSAccount() *types.IRODSAccount {
	negotiationRequired := false
	negotiationPolicy := types.CSNegotiationRequireTCP
	if types.AuthScheme(env.AuthenticationScheme) == types.AuthSchemePAM {
		negotiationRequired = true
		negotiationPolicy = types.CSNegotiationRequireSSL
	}

	if env.ClientServerNegotiation == "request_server_negotiation" {
		negotiationRequired = true
	}

	return &types.IRODSAccount{
		AuthenticationScheme:    types.AuthScheme(env.AuthenticationScheme),
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
		PamTTL:                  types.PamTTLDefault,
		SSLConfiguration: &types.IRODSSSLConfig{
			CACertificateFile:   env.SSLCACertificateFile,
			EncryptionKeySize:   env.EncryptionKeySize,
			EncryptionAlgorithm: env.EncryptionAlgorithm,
			SaltSize:            env.EncryptionSaltSize,
			HashRounds:          env.EncryptionNumHashRounds,
		},
	}
}

// ToJSON converts to JSON bytes
func (env *ICommandsEnvironment) ToJSON() ([]byte, error) {
	jsonBytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("JSON Marshal Error - %v", err)
	}

	return jsonBytes, nil
}

// ToFile saves to a file
func (env *ICommandsEnvironment) ToFile(envPath string) error {
	jsonByte, err := env.ToJSON()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(envPath, jsonByte, 0664)
}
