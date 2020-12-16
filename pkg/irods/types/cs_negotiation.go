package types

// CSNegotiation ...
type CSNegotiation string

const (
	// Negotiation request values
	NEGOTIATION_REQUIRE_SSL CSNegotiation = "CS_NEG_REQUIRE"
	NEGOTIATION_REQUIRE_TCP CSNegotiation = "CS_NEG_REFUSE"

	// Negotiation result (response) values
	NEGOTIATION_FAILURE CSNegotiation = "CS_NEG_FAILURE"
	NEGOTIATION_USE_SSL CSNegotiation = "CS_NEG_USE_SSL"
	NEGOTIATION_USE_TCP CSNegotiation = "CS_NEG_USE_TCP"
)
