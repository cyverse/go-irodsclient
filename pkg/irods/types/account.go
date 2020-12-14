package types

import (
	"strings"
)

// IRODSAccount contains irods login information
type IRODSAccount struct {
	AuthenticationScheme    string
	ClientServerNegotiation string
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
	authScheme string, password string,
	serverDN string) (*IRODSAccount, error) {
	return &IRODSAccount{
		AuthenticationScheme: strings.ToLower(authScheme),
		Host:                 host,
		Port:                 port,
		ClientUser:           user,
		ClientZone:           zone,
		ProxyUser:            user,
		ProxyZone:            zone,
		ServerDN:             serverDN,
		Password:             password,
	}, nil
}

// CreateIRODSProxyAccount creates IRODSAccount for proxy access
func CreateIRODSProxyAccount(host string, port int32, clientUser string, clientZone string,
	proxyUser string, proxyZone string,
	authScheme string, password string,
	serverDN string) (*IRODSAccount, error) {
	return &IRODSAccount{
		AuthenticationScheme: strings.ToLower(authScheme),
		Host:                 host,
		Port:                 port,
		ClientUser:           clientUser,
		ClientZone:           clientZone,
		ProxyUser:            proxyUser,
		ProxyZone:            proxyZone,
		ServerDN:             serverDN,
		Password:             password,
	}, nil
}

// UseProxyAccess returns whether it uses proxy access or not
func (account *IRODSAccount) UseProxyAccess() bool {
	if len(account.ProxyUser) > 0 && len(account.ClientUser) > 0 && account.ProxyUser != account.ClientUser {
		return true
	}
	return false
}
