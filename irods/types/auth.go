package types

import (
	"strings"
)

// AuthScheme defines Authentication Scheme
type AuthScheme string

const (
	// AuthSchemeNative uses Native authentication scheme
	AuthSchemeNative AuthScheme = "native"
	// AuthSchemeGSI uses GSI authentication scheme
	AuthSchemeGSI AuthScheme = "gsi"
	// AuthSchemePAM uses PAM authentication scheme
	AuthSchemePAM AuthScheme = "pam"
	// AuthSchemePAMPasswordAuthScheme uses PAM authentication scheme
	AuthSchemePAMPassword AuthScheme = "pam_password"
	// AuthSchemeUnknown is unknown scheme
	AuthSchemeUnknown AuthScheme = ""
)

// GetAuthScheme returns AuthScheme value from string
func GetAuthScheme(authScheme string) AuthScheme {
	switch strings.TrimSpace(strings.ToLower(authScheme)) {
	case string(AuthSchemeNative):
		return AuthSchemeNative
	case string(AuthSchemeGSI):
		return AuthSchemeGSI
	case string(AuthSchemePAM):
		return AuthSchemePAM
	case string(AuthSchemePAMPassword):
		return AuthSchemePAMPassword
	case string(AuthSchemeUnknown):
		fallthrough
	default:
		return AuthSchemeUnknown
	}
}

// IsPAM checks if the auth scheme is pam or pam_password
func (authScheme AuthScheme) IsPAM() bool {
	return authScheme == AuthSchemePAM || authScheme == AuthSchemePAMPassword
}
