package types

import (
	"regexp"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v3"
)

const (
	// PamTTLDefault is a default value for Pam TTL
	PamTTLDefault       int    = 1
	UsernameRegexString string = "^((\\w|[-.@])+)$"
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
	Password                string
	Ticket                  string
	DefaultResource         string
	PamTTL                  int
	PamToken                string
	SSLConfiguration        *IRODSSSLConfig
	ServerNameTLS           string // Optional TLS Server Name for SNI connection and TLS verification - defaults to Host
	SkipVerifyTLS           bool   // Skip TLS verification
}

// CreateIRODSAccount creates IRODSAccount
func CreateIRODSAccount(host string, port int, user string, zone string,
	authScheme AuthScheme, password string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              user,
		ClientZone:              zone,
		ProxyUser:               user,
		ProxyZone:               zone,
		Password:                password,
		Ticket:                  "",
		DefaultResource:         defaultResource,
		PamTTL:                  PamTTLDefault,
		PamToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// CreateIRODSAccountForTicket creates IRODSAccount
func CreateIRODSAccountForTicket(host string, port int, user string, zone string,
	authScheme AuthScheme, password string, ticket string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              user,
		ClientZone:              zone,
		ProxyUser:               user,
		ProxyZone:               zone,
		Password:                password,
		Ticket:                  ticket,
		DefaultResource:         defaultResource,
		PamTTL:                  PamTTLDefault,
		PamToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// CreateIRODSProxyAccount creates IRODSAccount for proxy access
func CreateIRODSProxyAccount(host string, port int, clientUser string, clientZone string,
	proxyUser string, proxyZone string,
	authScheme AuthScheme, password string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              clientUser,
		ClientZone:              clientZone,
		ProxyUser:               proxyUser,
		ProxyZone:               proxyZone,
		Password:                password,
		Ticket:                  "",
		DefaultResource:         defaultResource,
		PamTTL:                  PamTTLDefault,
		PamToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// CreateIRODSAccountFromYAML creates IRODSAccount from YAML
func CreateIRODSAccountFromYAML(yamlBytes []byte) (*IRODSAccount, error) {
	y := make(map[string]interface{})

	err := yaml.Unmarshal(yamlBytes, &y)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal yaml to map: %w", err)
	}

	authScheme := AuthSchemeNative
	if val, ok := y["auth_scheme"]; ok {
		authScheme = GetAuthScheme(val.(string))
		if authScheme == AuthSchemeUnknown {
			authScheme = AuthSchemeNative
		}
	}

	csNegotiation := false
	if val, ok := y["cs_negotiation"]; ok {
		csNegotiation = val.(bool)
	}

	csNegotiationPolicy := CSNegotiationDontCare
	if val, ok := y["cs_negotiation_policy"]; ok {
		csNegotiationPolicy, err = GetCSNegotiationRequire(val.(string))
		if err != nil {
			csNegotiationPolicy = CSNegotiationDontCare
		}
	}

	host := make(map[string]interface{})
	if val, ok := y["host"]; ok {
		host = val.(map[string]interface{})
	}

	hostname := ""
	if val, ok := host["hostname"]; ok {
		hostname = val.(string)
	}

	port := 1247
	if val, ok := host["port"]; ok {
		port = val.(int)
	}

	defaultResource := ""
	if val, ok := y["default_resource"]; ok {
		defaultResource = val.(string)
	}

	// proxy user
	proxyUser := make(map[string]interface{})
	if val, ok := y["proxy_user"]; ok {
		proxyUser = val.(map[string]interface{})
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

	ticket := ""
	if val, ok := proxyUser["ticket"]; ok {
		ticket = val.(string)
	}

	// client user
	clientUser := make(map[string]interface{})
	if val, ok := y["client_user"]; ok {
		clientUser = val.(map[string]interface{})
	}

	clientUsername := ""
	if val, ok := clientUser["username"]; ok {
		clientUsername = val.(string)
	}

	clientZone := ""
	if val, ok := clientUser["zone"]; ok {
		clientZone = val.(string)
	}

	if val, ok := clientUser["ticket"]; ok {
		ticket = val.(string)
	}

	// normal user
	user := make(map[string]interface{})
	if val, ok := y["user"]; ok {
		user = val.(map[string]interface{})
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

	if val, ok := user["ticket"]; ok {
		ticket = val.(string)
	}

	// PAM Configuration
	pamConfig := make(map[string]interface{})
	if val, ok := y["pam"]; ok {
		pamConfig = val.(map[string]interface{})
	}

	pamTTL := 0
	if val, ok := pamConfig["ttl"]; ok {
		pamTTL = val.(int)
	}

	pamToken := ""
	if val, ok := pamConfig["token"]; ok {
		pamToken = val.(string)
	}

	// SSL Configuration
	hasSSLConfig := false
	sslConfig := make(map[string]interface{})
	if val, ok := y["ssl"]; ok {
		sslConfig = val.(map[string]interface{})
		hasSSLConfig = true
	}

	caCertFile := ""
	if val, ok := sslConfig["ca_cert_file"]; ok {
		caCertFile = val.(string)
	}

	caCertPath := ""
	if val, ok := sslConfig["ca_cert_path"]; ok {
		caCertPath = val.(string)
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
		irodsSSLConfig, err = CreateIRODSSSLConfig(caCertFile, caCertPath, keySize, algorithm, saltSize, hashRounds)
		if err != nil {
			return nil, xerrors.Errorf("failed to create irods ssl config: %w", err)
		}
	}

	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: csNegotiation,
		CSNegotiationPolicy:     csNegotiationPolicy,
		Host:                    hostname,
		Port:                    port,
		ClientUser:              clientUsername,
		ClientZone:              clientZone,
		ProxyUser:               proxyUsername,
		ProxyZone:               proxyZone,
		Password:                proxyPassword,
		Ticket:                  ticket,
		DefaultResource:         defaultResource,
		PamTTL:                  pamTTL,
		PamToken:                pamToken,
		SSLConfiguration:        irodsSSLConfig,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// SetSSLConfiguration sets SSL Configuration
func (account *IRODSAccount) SetSSLConfiguration(sslConf *IRODSSSLConfig) {
	account.SSLConfiguration = sslConf
}

// SetCSNegotiation sets CSNegotiation policy
func (account *IRODSAccount) SetCSNegotiation(requireNegotiation bool, requirePolicy CSNegotiationRequire) {
	account.ClientServerNegotiation = requireNegotiation
	account.CSNegotiationPolicy = requirePolicy

	account.FixAuthConfiguration()
}

// UseProxyAccess returns whether it uses proxy access or not
func (account *IRODSAccount) UseProxyAccess() bool {
	return len(account.ProxyUser) > 0 && len(account.ClientUser) > 0 && account.ProxyUser != account.ClientUser
}

// UseTicket returns whether it uses ticket for access control
func (account *IRODSAccount) UseTicket() bool {
	return len(account.Ticket) > 0
}

// Validate validates iRODS account
func (account *IRODSAccount) Validate() error {
	if len(account.Host) == 0 {
		return xerrors.Errorf("empty host")
	}

	if account.Port <= 0 {
		return xerrors.Errorf("empty port")
	}

	if len(account.ProxyUser) == 0 {
		return xerrors.Errorf("empty user")
	}

	err := account.validateUsername(account.ProxyUser)
	if err != nil {
		return xerrors.Errorf("failed to validate username %s: %w", account.ProxyUser, err)
	}

	if len(account.ClientUser) > 0 {
		err = account.validateUsername(account.ClientUser)
		if err != nil {
			return xerrors.Errorf("failed to validate username %s: %w", account.ProxyUser, err)
		}
	}

	if len(account.ProxyZone) == 0 {
		return xerrors.Errorf("empty zone")
	}

	if account.AuthenticationScheme == AuthSchemeUnknown {
		return xerrors.Errorf("unknown authentication scheme")
	}

	if account.AuthenticationScheme != AuthSchemeNative && account.CSNegotiationPolicy != CSNegotiationRequireSSL {
		return xerrors.Errorf("SSL is required for non-native authentication scheme")
	}

	if account.CSNegotiationPolicy == CSNegotiationRequireSSL && !account.ClientServerNegotiation {
		return xerrors.Errorf("client-server negotiation is required for SSL")
	}

	if account.CSNegotiationPolicy == CSNegotiationRequireSSL && account.SSLConfiguration == nil {
		return xerrors.Errorf("SSL configuration is empty")
	}

	return nil
}

func (account *IRODSAccount) validateUsername(username string) error {
	if len(username) >= common.MaxNameLength {
		return xerrors.Errorf("username too long")
	}

	if username == "." || username == ".." {
		return xerrors.Errorf("invalid username")
	}

	usernameRegEx, err := regexp.Compile(UsernameRegexString)
	if err != nil {
		return xerrors.Errorf("failed to compile regex: %w", err)
	}

	if !usernameRegEx.Match([]byte(username)) {
		return xerrors.Errorf("invalid username, containing invalid chars")
	}
	return nil
}

func (account *IRODSAccount) FixAuthConfiguration() {
	if account.AuthenticationScheme == AuthSchemeUnknown {
		account.AuthenticationScheme = AuthSchemeNative
	}

	if account.AuthenticationScheme != AuthSchemeNative {
		account.CSNegotiationPolicy = CSNegotiationRequireSSL
	}

	if account.CSNegotiationPolicy == CSNegotiationRequireSSL {
		account.ClientServerNegotiation = true
	}
}

func (account *IRODSAccount) GetRedacted() *IRODSAccount {
	account2 := IRODSAccount{}
	account2 = *account
	account2.Password = "<Redacted>"
	account2.PamToken = "<Redacted>"
	account2.Ticket = "<Redacted>"

	return &account2
}
