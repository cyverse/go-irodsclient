package api

import (
	"fmt"
)

// negotiation constants
const (
	// Token sent to the server to request negotiation
	REQUEST_NEGOTIATION string = "request_server_negotiation"

	// Negotiation request values
	REQUIRE_SSL string = "CS_NEG_REQUIRE"
	REQUIRE_TCP string = "CS_NEG_REFUSE"

	// Negotiation result (response) values
	FAILURE string = "CS_NEG_FAILURE"
	USE_SSL string = "CS_NEG_USE_SSL"
	USE_TCP string = "CS_NEG_USE_TCP"

	// Keywords
	CS_NEG_SID_KW    string = "cs_neg_sid_kw"
	CS_NEG_RESULT_KW string = "cs_neg_result_kw"
)

// PerformNegotiation performs negotiation
func PerformNegotiation(clientPolicy string, serverPolicy string) (string, int) {
	if REQUIRE_SSL == clientPolicy || REQUIRE_SSL == serverPolicy {
		if REQUIRE_TCP == clientPolicy || REQUIRE_TCP == serverPolicy {
			return FAILURE, 0
		}
		return USE_SSL, 1
	}
	return USE_TCP, 1
}

// ValidatePolicy validates policy
func ValidatePolicy(policy string) error {
	switch policy {
	case REQUIRE_SSL, REQUIRE_TCP:
		return nil
	default:
		return fmt.Errorf("Invalid client-server negotiation policy: %s", policy)
	}
}
