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
	case string(AuthSchemePAM), "pam_password":
		return AuthSchemePAM
	case string(AuthSchemeUnknown):
		fallthrough
	default:
		return AuthSchemeUnknown
	}
}
