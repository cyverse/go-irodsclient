package types

import (
	"fmt"
	"strings"
)

// CSNegotiation defines to perform Negotiation or not
type CSNegotiation string

const (
	// CSNegotiationRequestServerNegotiation presents negotiation is required
	CSNegotiationServerNegotiation CSNegotiation = "REQUEST_SERVER_NEGOTIATION"
	CSNegotiationOff               CSNegotiation = "OFF"
)

// GetCSNegotiation returns CSNegotiation value from string
func GetCSNegotiation(negotiation string) (CSNegotiation, error) {
	csNegotiation := CSNegotiationOff
	var err error = nil
	switch strings.TrimSpace(strings.ToUpper(negotiation)) {
	case string(CSNegotiationServerNegotiation):
		csNegotiation = CSNegotiationServerNegotiation
	case string(CSNegotiationOff), "":
		csNegotiation = CSNegotiationOff
	default:
		csNegotiation = CSNegotiationOff
		err = fmt.Errorf("cannot parse string %q", negotiation)
	}

	return csNegotiation, err
}

// CSNegotiationPolicyRequest defines Negotiation policy request
type CSNegotiationPolicyRequest string

const (
	// CSNegotiationPolicyRequestTCP requests Plain TCP connection
	CSNegotiationPolicyRequestTCP CSNegotiationPolicyRequest = "CS_NEG_REFUSE"
	// CSNegotiationPolicyRequestSSL requests SSL connection
	CSNegotiationPolicyRequestSSL CSNegotiationPolicyRequest = "CS_NEG_REQUIRE"
	// CSNegotiationPolicyRequestDontCare requests any of TCP or SSL connection
	CSNegotiationPolicyRequestDontCare CSNegotiationPolicyRequest = "CS_NEG_DONT_CARE"
)

// GetCSNegotiationPolicyRequest returns CSNegotiationPolicyRequest value from string
func GetCSNegotiationPolicyRequest(request string) (CSNegotiationPolicyRequest, error) {
	csNegotiationPolicyRequest := CSNegotiationPolicyRequestTCP
	var err error = nil
	switch strings.TrimSpace(strings.ToUpper(request)) {
	case string(CSNegotiationPolicyRequestTCP), "TCP":
		csNegotiationPolicyRequest = CSNegotiationPolicyRequestTCP
	case string(CSNegotiationPolicyRequestSSL), "SSL":
		csNegotiationPolicyRequest = CSNegotiationPolicyRequestSSL
	case string(CSNegotiationPolicyRequestDontCare), "DONT_CARE", "":
		csNegotiationPolicyRequest = CSNegotiationPolicyRequestDontCare
	default:
		csNegotiationPolicyRequest = CSNegotiationPolicyRequestTCP
		err = fmt.Errorf("cannot parse string %q", request)
	}

	return csNegotiationPolicyRequest, err
}

// CSNegotiationResult defines Negotiation result
type CSNegotiationResult string

const (
	// CSNegotiationFailure presents negotiation is failed
	CSNegotiationFailure CSNegotiationResult = "CS_NEG_FAILURE"
	// CSNegotiationUseTCP uses Plain TCP connection
	CSNegotiationUseTCP CSNegotiationResult = "CS_NEG_USE_TCP"
	// CSNegotiationUseSSL uses SSL connection
	CSNegotiationUseSSL CSNegotiationResult = "CS_NEG_USE_SSL"
)

// GetCSNegotiationResult returns CSNegotiationResult value from string
func GetCSNegotiationResult(policy string) (CSNegotiationResult, error) {
	csNegotiationResult := CSNegotiationFailure
	var err error = nil
	switch strings.TrimSpace(strings.ToUpper(policy)) {
	case string(CSNegotiationUseTCP), "TCP":
		csNegotiationResult = CSNegotiationUseTCP
	case string(CSNegotiationUseSSL), "SSL":
		csNegotiationResult = CSNegotiationUseSSL
	case string(CSNegotiationFailure), "FAILURE":
		csNegotiationResult = CSNegotiationFailure
	default:
		csNegotiationResult = CSNegotiationFailure
		err = fmt.Errorf("cannot parse string %q", policy)
	}

	return csNegotiationResult, err
}

// PerformCSNegotiation performs CSNegotiation and returns the policy determined
func PerformCSNegotiation(clientRequest CSNegotiationPolicyRequest, serverRequest CSNegotiationPolicyRequest) CSNegotiationResult {
	if serverRequest == CSNegotiationPolicyRequestDontCare {
		switch clientRequest {
		case CSNegotiationPolicyRequestDontCare, CSNegotiationPolicyRequestTCP:
			return CSNegotiationUseTCP
		case CSNegotiationPolicyRequestSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}

	if clientRequest == CSNegotiationPolicyRequestDontCare {
		switch serverRequest {
		case CSNegotiationPolicyRequestTCP:
			return CSNegotiationUseTCP
		case CSNegotiationPolicyRequestSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}

	if clientRequest == serverRequest {
		switch clientRequest {
		case CSNegotiationPolicyRequestTCP:
			return CSNegotiationUseTCP
		case CSNegotiationPolicyRequestSSL:
			return CSNegotiationUseSSL
		default:
			return CSNegotiationFailure
		}
	}
	return CSNegotiationFailure
}
