package types

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

const (
	PamTTLDefault int = 1
)

// IRODSAccount contains irods login information
type IRODSAccount struct {
	AuthenticationScheme    AuthScheme
	ClientServerNegotiation bool
	CSNegotiationPolicy     CSNegotiationRequire
	Host                    string
	Port                    int
	ClientUser              string
	ClientZone              string
	ProxyUser               string
	ProxyZone               string
	ServerDN                string
	Password                string
	PamTTL                  int
	SSLConfiguration        *IRODSSSLConfig
}

// CreateIRODSAccount creates IRODSAccount
func CreateIRODSAccount(host string, port int, user string, zone string,
	authScheme AuthScheme, password string,
	serverDN string) (*IRODSAccount, error) {
	return &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationRequireTCP,
		Host:                    host,
		Port:                    port,
		ClientUser:              user,
		ClientZone:              zone,
		ProxyUser:               user,
		ProxyZone:               zone,
		ServerDN:                serverDN,
		Password:                password,
		PamTTL:                  PamTTLDefault,
		SSLConfiguration:        nil,
	}, nil
}

// CreateIRODSProxyAccount creates IRODSAccount for proxy access
func CreateIRODSProxyAccount(host string, port int, clientUser string, clientZone string,
	proxyUser string, proxyZone string,
	authScheme AuthScheme, password string) (*IRODSAccount, error) {
	return &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationRequireTCP,
		Host:                    host,
		Port:                    port,
		ClientUser:              clientUser,
		ClientZone:              clientZone,
		ProxyUser:               proxyUser,
		ProxyZone:               proxyZone,
		Password:                password,
		PamTTL:                  PamTTLDefault,
		SSLConfiguration:        nil,
	}, nil
}

// CreateIRODSAccountFromYAML creates IRODSAccount from YAML
func CreateIRODSAccountFromYAML(yamlBytes []byte) (*IRODSAccount, error) {
	y := make(map[interface{}]interface{})

	err := yaml.Unmarshal(yamlBytes, &y)
	if err != nil {
		return nil, fmt.Errorf("YAML Unmarshal Error - %v", err)
	}

	authScheme := AuthSchemeNative
	if val, ok := y["auth_scheme"]; ok {
		authScheme, err = GetAuthScheme(val.(string))
		if err != nil {
			authScheme = AuthSchemeNative
		}
	}

	csNegotiation := false
	if val, ok := y["cs_negotiation"]; ok {
		csNegotiation = val.(bool)
	}

	csNegotiationPolicy := CSNegotiationRequireTCP
	if val, ok := y["cs_negotiation_policy"]; ok {
		csNegotiationPolicy, err = GetCSNegotiationRequire(val.(string))
		if err != nil {
			csNegotiationPolicy = CSNegotiationRequireTCP
		}
	}

	serverDN := ""
	if val, ok := y["server_dn"]; ok {
		serverDN = val.(string)
	}

	host := make(map[interface{}]interface{})
	if val, ok := y["host"]; ok {
		host = val.(map[interface{}]interface{})
	}

	hostname := ""
	if val, ok := host["hostname"]; ok {
		hostname = val.(string)
	}

	port := 1247
	if val, ok := host["port"]; ok {
		port = val.(int)
	}

	// proxy user
	proxyUser := make(map[interface{}]interface{})
	if val, ok := y["proxy_user"]; ok {
		proxyUser = val.(map[interface{}]interface{})
	}

	proxyUsername := ""
	if val, ok := proxyUser["username"]; ok {
		proxyUsername = val.(string)
	}

	proxyPassword := ""
	if val, ok := proxyUser["password"]; ok {
		proxyPassword = val.(string)
	}

	proxyZone := ""
	if val, ok := proxyUser["zone"]; ok {
		proxyZone = val.(string)
	}

	// client user
	clientUser := make(map[interface{}]interface{})
	if val, ok := y["client_user"]; ok {
		clientUser = val.(map[interface{}]interface{})
	}

	clientUsername := ""
	if val, ok := clientUser["username"]; ok {
		clientUsername = val.(string)
	}

	clientZone := ""
	if val, ok := clientUser["zone"]; ok {
		clientZone = val.(string)
	}

	// normal user
	user := make(map[interface{}]interface{})
	if val, ok := y["user"]; ok {
		user = val.(map[interface{}]interface{})
	}

	if val, ok := user["username"]; ok {
		proxyUsername = val.(string)
		clientUsername = proxyUsername

	}

	if val, ok := user["password"]; ok {
		proxyPassword = val.(string)
	}

	if val, ok := user["zone"]; ok {
		proxyZone = val.(string)
		clientZone = proxyZone
	}

	// PAM Configuration
	pamConfig := make(map[interface{}]interface{})
	if val, ok := y["pam"]; ok {
		pamConfig = val.(map[interface{}]interface{})
	}

	pamTTL := 0
	if val, ok := pamConfig["ttl"]; ok {
		pamTTL = val.(int)
	}

	// SSL Configuration
	hasSSLConfig := false
	sslConfig := make(map[interface{}]interface{})
	if val, ok := y["ssl"]; ok {
		sslConfig = val.(map[interface{}]interface{})
		hasSSLConfig = true
	}

	caCert := ""
	if val, ok := sslConfig["ca_cert_file"]; ok {
		caCert = val.(string)
	}

	keySize := 0
	if val, ok := sslConfig["key_size"]; ok {
		keySize = val.(int)
	}

	algorithm := ""
	if val, ok := sslConfig["algorithm"]; ok {
		algorithm = val.(string)
	}

	saltSize := 0
	if val, ok := sslConfig["salt_size"]; ok {
		saltSize = val.(int)
	}

	hashRounds := 0
	if val, ok := sslConfig["hash_rounds"]; ok {
		hashRounds = val.(int)
	}

	var irodsSSLConfig *IRODSSSLConfig = nil
	if hasSSLConfig {
		irodsSSLConfig, err = CreateIRODSSSLConfig(caCert, keySize, algorithm, saltSize, hashRounds)
		if err != nil {
			return nil, err
		}
	}

	return &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: csNegotiation,
		CSNegotiationPolicy:     csNegotiationPolicy,
		Host:                    hostname,
		Port:                    port,
		ClientUser:              clientUsername,
		ClientZone:              clientZone,
		ProxyUser:               proxyUsername,
		ProxyZone:               proxyZone,
		ServerDN:                serverDN,
		Password:                proxyPassword,
		PamTTL:                  pamTTL,
		SSLConfiguration:        irodsSSLConfig,
	}, nil
}

// SetSSLConfiguration sets SSL Configuration
func (account *IRODSAccount) SetSSLConfiguration(sslConf *IRODSSSLConfig) {
	account.SSLConfiguration = sslConf
}

// UseProxyAccess returns whether it uses proxy access or not
func (account *IRODSAccount) UseProxyAccess() bool {
	if len(account.ProxyUser) > 0 && len(account.ClientUser) > 0 && account.ProxyUser != account.ClientUser {
		return true
	}
	return false
}

// MaskSensitiveData returns IRODSAccount object with sensitive data masked
func (account *IRODSAccount) MaskSensitiveData() *IRODSAccount {
	maskedAccount := *account
	maskedAccount.Password = "<password masked>"

	return &maskedAccount
}
