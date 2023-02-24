package types

import (
	"strings"

	"golang.org/x/xerrors"
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
)

// GetAuthScheme returns AuthScheme value from string
func GetAuthScheme(authScheme string) (AuthScheme, error) {
	switch strings.TrimSpace(strings.ToLower(authScheme)) {
	case string(AuthSchemeNative):
		return AuthSchemeNative, nil
	case string(AuthSchemeGSI):
		return AuthSchemeGSI, nil
	case string(AuthSchemePAM):
		return AuthSchemePAM, nil
	default:
		return AuthSchemeNative, xerrors.Errorf("cannot parse string %s", authScheme)
	}
}
