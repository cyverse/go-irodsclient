package types

import (
	"fmt"
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
)

// GetAuthScheme returns AuthScheme value from string
func GetAuthScheme(authScheme string) (AuthScheme, error) {
	scheme := AuthSchemeNative
	var err error = nil
	switch strings.TrimSpace(strings.ToLower(authScheme)) {
	case string(AuthSchemeNative):
		scheme = AuthSchemeNative
	case string(AuthSchemeGSI):
		scheme = AuthSchemeGSI
	case string(AuthSchemePAM):
		scheme = AuthSchemePAM
	default:
		scheme = AuthSchemeNative
		err = fmt.Errorf("Cannot parse string %s", authScheme)
	}

	return scheme, err
}
