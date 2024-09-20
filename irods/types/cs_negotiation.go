package types

import (
	"strings"
)

// CSNegotiation defines to perform Negotiation or not
type CSNegotiation string

const (
	// CSNegotiationRequestServerNegotiation presents negotiation is required
	CSNegotiationRequestServerNegotiation CSNegotiation = "request_server_negotiation"
	CSNegotiationOff                      CSNegotiation = "off"
)

// GetCSNegotiation returns CSNegotiation value from string
func GetCSNegotiation(negotiation string) CSNegotiation {
	switch strings.TrimSpace(strings.ToLower(negotiation)) {
	case string(CSNegotiationRequestServerNegotiation), "request":
		return CSNegotiationRequestServerNegotiation
	case string(CSNegotiationOff), "none", "":
		return CSNegotiationOff
	default:
		return CSNegotiationOff
	}
}

// IsNegotiationRequired checks if negotiation is required
func (negotiation CSNegotiation) IsNegotiationRequired() bool {
	return negotiation == CSNegotiationRequestServerNegotiation
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
func GetCSNegotiationPolicyRequest(request string) CSNegotiationPolicyRequest {
	switch strings.TrimSpace(strings.ToUpper(request)) {
	case string(CSNegotiationPolicyRequestTCP), "TCP":
		return CSNegotiationPolicyRequestTCP
	case string(CSNegotiationPolicyRequestSSL), "SSL":
		return CSNegotiationPolicyRequestSSL
	case string(CSNegotiationPolicyRequestDontCare), "DONT_CARE", "":
		return CSNegotiationPolicyRequestDontCare
	default:
		return CSNegotiationPolicyRequestTCP
	}
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
func GetCSNegotiationResult(policy string) CSNegotiationResult {
	switch strings.TrimSpace(strings.ToUpper(policy)) {
	case string(CSNegotiationUseTCP), "TCP":
		return CSNegotiationUseTCP
	case string(CSNegotiationUseSSL), "SSL":
		return CSNegotiationUseSSL
	case string(CSNegotiationFailure), "FAILURE":
		return CSNegotiationFailure
	default:
		return CSNegotiationFailure
	}
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
