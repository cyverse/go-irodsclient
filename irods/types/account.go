package types

import (
	"fmt"
	"regexp"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

const (
	// PamTTLDefault is a default value for Pam TTL
	PamTTLDefault       int    = 0 // sever decides
	UsernameRegexString string = "^((\\w|[-.@])+)$"
	HashSchemeDefault   string = "SHA256"
)

// IRODSAccount contains irods login information
type IRODSAccount struct {
	AuthenticationScheme    AuthScheme
	ClientServerNegotiation bool
	CSNegotiationPolicy     CSNegotiationPolicyRequest
	Host                    string
	Port                    int
	ClientUser              string
	ClientZone              string
	ProxyUser               string
	ProxyZone               string
	Password                string
	Ticket                  string
	DefaultResource         string
	DefaultHashScheme       string
	PamTTL                  int
	PAMToken                string
	SSLConfiguration        *IRODSSSLConfig
}

// CreateIRODSAccount creates IRODSAccount
func CreateIRODSAccount(host string, port int, username string, zoneName string,
	authScheme AuthScheme, password string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationPolicyRequestDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              username,
		ClientZone:              zoneName,
		ProxyUser:               username,
		ProxyZone:               zoneName,
		Password:                password,
		Ticket:                  "",
		DefaultResource:         defaultResource,
		DefaultHashScheme:       HashSchemeDefault,
		PamTTL:                  PamTTLDefault,
		PAMToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// CreateIRODSAccountForTicket creates IRODSAccount
func CreateIRODSAccountForTicket(host string, port int, username string, zoneName string,
	authScheme AuthScheme, password string, ticket string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationPolicyRequestDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              username,
		ClientZone:              zoneName,
		ProxyUser:               username,
		ProxyZone:               zoneName,
		Password:                password,
		Ticket:                  ticket,
		DefaultResource:         defaultResource,
		DefaultHashScheme:       HashSchemeDefault,
		PamTTL:                  PamTTLDefault,
		PAMToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// CreateIRODSProxyAccount creates IRODSAccount for proxy access
func CreateIRODSProxyAccount(host string, port int, clientUsername string, clientZoneName string,
	proxyUsername string, proxyZoneName string,
	authScheme AuthScheme, password string, defaultResource string) (*IRODSAccount, error) {
	account := &IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: false,
		CSNegotiationPolicy:     CSNegotiationPolicyRequestDontCare,
		Host:                    host,
		Port:                    port,
		ClientUser:              clientUsername,
		ClientZone:              clientZoneName,
		ProxyUser:               proxyUsername,
		ProxyZone:               proxyZoneName,
		Password:                password,
		Ticket:                  "",
		DefaultResource:         defaultResource,
		DefaultHashScheme:       HashSchemeDefault,
		PamTTL:                  PamTTLDefault,
		PAMToken:                "",
		SSLConfiguration:        nil,
	}

	account.FixAuthConfiguration()

	return account, nil
}

// SetSSLConfiguration sets SSL Configuration
func (account *IRODSAccount) SetSSLConfiguration(sslConf *IRODSSSLConfig) {
	account.SSLConfiguration = sslConf
}

// SetCSNegotiation sets CSNegotiation policy
func (account *IRODSAccount) SetCSNegotiation(requireNegotiation bool, requirePolicy CSNegotiationPolicyRequest) {
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

func (account *IRODSAccount) IsAnonymousUser() bool {
	return account.ClientUser == "anonymous"
}

// GetHomeDirPath returns user's home directory path
func (account *IRODSAccount) GetHomeDirPath() string {
	if account.IsAnonymousUser() {
		return fmt.Sprintf("/%s/home", account.ClientZone)
	}

	return fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)
}

// Validate validates iRODS account
func (account *IRODSAccount) Validate() error {
	if len(account.Host) == 0 {
		return xerrors.Errorf("empty host: %w", NewConnectionConfigError(account))
	}

	if account.Port <= 0 {
		return xerrors.Errorf("empty port: %w", NewConnectionConfigError(account))
	}

	if len(account.ProxyUser) == 0 {
		return xerrors.Errorf("empty user: %w", NewConnectionConfigError(account))
	}

	err := account.validateUsername(account.ProxyUser)
	if err != nil {
		return xerrors.Errorf("failed to validate username %q: %w", account.ProxyUser, err)
	}

	if len(account.ClientUser) > 0 {
		err = account.validateUsername(account.ClientUser)
		if err != nil {
			return xerrors.Errorf("failed to validate username %q: %w", account.ClientUser, err)
		}
	}

	if len(account.ProxyZone) == 0 {
		return xerrors.Errorf("empty zone: %w", NewConnectionConfigError(account))
	}

	if account.AuthenticationScheme == AuthSchemeUnknown {
		return xerrors.Errorf("unknown authentication scheme: %w", NewConnectionConfigError(account))
	}

	if account.AuthenticationScheme != AuthSchemeNative && account.CSNegotiationPolicy != CSNegotiationPolicyRequestSSL {
		return xerrors.Errorf("SSL is required for non-native authentication scheme: %w", NewConnectionConfigError(account))
	}

	if account.CSNegotiationPolicy == CSNegotiationPolicyRequestSSL && !account.ClientServerNegotiation {
		return xerrors.Errorf("client-server negotiation is required for SSL: %w", NewConnectionConfigError(account))
	}

	if account.CSNegotiationPolicy == CSNegotiationPolicyRequestSSL && account.SSLConfiguration == nil {
		return xerrors.Errorf("SSL configuration is empty: %w", NewConnectionConfigError(account))
	}

	if account.SSLConfiguration != nil {
		err = account.SSLConfiguration.Validate()
		if err != nil {
			return xerrors.Errorf("failed to validate SSL configuration (%s): %w", err.Error(), NewConnectionConfigError(account))
		}
	}

	return nil
}

func (account *IRODSAccount) validateUsername(username string) error {
	if len(username) >= common.MaxNameLength {
		return xerrors.Errorf("username too long: %w", NewConnectionConfigError(account))
	}

	if username == "." || username == ".." {
		return xerrors.Errorf("invalid username: %w", NewConnectionConfigError(account))
	}

	usernameRegEx, err := regexp.Compile(UsernameRegexString)
	if err != nil {
		return xerrors.Errorf("failed to compile regex (err %s): %w", err.Error(), NewConnectionConfigError(account))
	}

	if !usernameRegEx.Match([]byte(username)) {
		return xerrors.Errorf("invalid username, containing invalid chars: %w", NewConnectionConfigError(account))
	}
	return nil
}

func (account *IRODSAccount) FixAuthConfiguration() {
	if account.AuthenticationScheme == AuthSchemeUnknown {
		account.AuthenticationScheme = AuthSchemeNative
	}

	if account.AuthenticationScheme != AuthSchemeNative {
		account.CSNegotiationPolicy = CSNegotiationPolicyRequestSSL
	}

	if account.CSNegotiationPolicy == CSNegotiationPolicyRequestSSL {
		account.ClientServerNegotiation = true
	}

	if len(account.ProxyUser) == 0 {
		account.ProxyUser = account.ClientUser
	}

	if len(account.ClientUser) == 0 {
		account.ClientUser = account.ProxyUser
	}

	if len(account.ProxyZone) == 0 {
		account.ProxyZone = account.ClientZone
	}

	if len(account.ClientZone) == 0 {
		account.ClientZone = account.ProxyZone
	}
}

func (account *IRODSAccount) GetRedacted() *IRODSAccount {
	account2 := *account
	account2.Password = "<Redacted>"
	account2.PAMToken = "<Redacted>"
	account2.Ticket = "<Redacted>"

	return &account2
}
