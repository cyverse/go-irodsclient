package types

import (
	"fmt"
	"strings"
)

// CSNegotiationRequire defines Negotiation request
type CSNegotiationRequire string

const (
	// CSNegotiationRequireTCP requires Plain TCP connection
	CSNegotiationRequireTCP CSNegotiationRequire = "CS_NEG_REFUSE"
	// CSNegotiationRequireSSL requires SSL connection
	CSNegotiationRequireSSL CSNegotiationRequire = "CS_NEG_REQUIRE"
	// CSNegotiationDontCare requires any of TCP or SSL connection
	CSNegotiationDontCare CSNegotiationRequire = "CS_NEG_DONT_CARE"
)

// GetCSNegotiationRequire returns CSNegotiationRequire value from string
func GetCSNegotiationRequire(require string) (CSNegotiationRequire, error) {
	csNegotiationPolicy := CSNegotiationRequireTCP
	var err error = nil
	switch strings.TrimSpace(strings.ToUpper(require)) {
	case string(CSNegotiationRequireTCP), "TCP":
		csNegotiationPolicy = CSNegotiationRequireTCP
	case string(CSNegotiationRequireSSL), "SSL":
		csNegotiationPolicy = CSNegotiationRequireSSL
	case string(CSNegotiationDontCare), "DONT_CARE", "":
		csNegotiationPolicy = CSNegotiationDontCare
	default:
		csNegotiationPolicy = CSNegotiationRequireTCP
		err = fmt.Errorf("cannot parse string %s", require)
	}

	return csNegotiationPolicy, err
}

// CSNegotiationPolicy defines Negotiation result
type CSNegotiationPolicy string

const (
	// CSNegotiationFailure presents negotiation is failed
	CSNegotiationFailure CSNegotiationPolicy = "CS_NEG_FAILURE"
	// CSNegotiationUseTCP uses Plain TCP connection
	CSNegotiationUseTCP CSNegotiationPolicy = "CS_NEG_USE_TCP"
	// CSNegotiationUseSSL uses SSL connection
	CSNegotiationUseSSL CSNegotiationPolicy = "CS_NEG_USE_SSL"
)

// GetCSNegotiationPolicy returns CSNegotiationPolicy value from string
func GetCSNegotiationPolicy(policy string) (CSNegotiationPolicy, error) {
	csNegotiationPolicy := CSNegotiationFailure
	var err error = nil
	switch strings.TrimSpace(strings.ToUpper(policy)) {
	case string(CSNegotiationUseTCP), "TCP":
		csNegotiationPolicy = CSNegotiationUseTCP
	case string(CSNegotiationUseSSL), "SSL":
		csNegotiationPolicy = CSNegotiationUseSSL
	default:
		csNegotiationPolicy = CSNegotiationFailure
		err = fmt.Errorf("cannot parse string %s", policy)
	}

	return csNegotiationPolicy, err
}

// PerformCSNegotiation performs CSNegotiation and returns the policy determined
func PerformCSNegotiation(clientRequest CSNegotiationRequire, serverRequest CSNegotiationRequire) CSNegotiationPolicy {
	if serverRequest == CSNegotiationDontCare {
		switch clientRequest {
		case CSNegotiationDontCare, CSNegotiationRequireTCP:
			return CSNegotiationUseTCP
		case CSNegotiationRequireSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}

	if clientRequest == CSNegotiationDontCare {
		switch serverRequest {
		case CSNegotiationRequireTCP:
			return CSNegotiationUseTCP
		case CSNegotiationRequireSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}

	if clientRequest == serverRequest {
		switch clientRequest {
		case CSNegotiationRequireTCP:
			return CSNegotiationUseTCP
		case CSNegotiationRequireSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}
	return CSNegotiationFailure
}
