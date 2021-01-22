package types

import (
	"gopkg.in/yaml.v2"
)

// IRODSAccount contains irods login information
type IRODSAccount struct {
	AuthenticationScheme    AuthScheme
	ClientServerNegotiation bool
	CSNegotiationPolicy     CSNegotiationRequire
	Host                    string
	Port                    int32
	ClientUser              string
	ClientZone              string
	ProxyUser               string
	ProxyZone               string
	ServerDN                string
	Password                string
}

// CreateIRODSAccount creates IRODSAccount
func CreateIRODSAccount(host string, port int32, user string, zone string,
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
	}, nil
}

// CreateIRODSProxyAccount creates IRODSAccount for proxy access
func CreateIRODSProxyAccount(host string, port int32, clientUser string, clientZone string,
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
	}, nil
}

// CreateIRODSAccountFromYAML creates IRODSAccount from YAML
func CreateIRODSAccountFromYAML(yamlBytes []byte) (*IRODSAccount, error) {
	y := make(map[interface{}]interface{})

	err := yaml.Unmarshal(yamlBytes, &y)
	if err != nil {
		return nil, err
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

	return &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: csNegotiation,
		CSNegotiationPolicy:     csNegotiationPolicy,
		Host:                    hostname,
		Port:                    int32(port),
		ClientUser:              clientUsername,
		ClientZone:              clientZone,
		ProxyUser:               proxyUsername,
		ProxyZone:               proxyZone,
		ServerDN:                serverDN,
		Password:                proxyPassword,
	}, nil
}

// UseProxyAccess returns whether it uses proxy access or not
func (account *IRODSAccount) UseProxyAccess() bool {
	if len(account.ProxyUser) > 0 && len(account.ClientUser) > 0 && account.ProxyUser != account.ClientUser {
		return true
	}
	return false
}
