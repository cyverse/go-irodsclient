package types

import (
	"fmt"
)

// IRODSTicketRestrictions contains irods ticket restriction information
type IRODSTicketRestrictions struct {
	// AllowedHosts is a list of allowed hosts
	AllowedHosts []string
	// AllowedUserName is a list of allowed user names
	AllowedUserNames []string
	// AllowedGroupNames is a list of allowed group names
	AllowedGroupNames []string
}

// ToString stringifies the object
func (ticket *IRODSTicketRestrictions) ToString() string {
	return fmt.Sprintf("<IRODSTicketRestrictions %v %v %v>", ticket.AllowedHosts, ticket.AllowedUserNames, ticket.AllowedGroupNames)
}
