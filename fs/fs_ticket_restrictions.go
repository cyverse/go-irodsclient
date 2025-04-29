package fs

import (
	"fmt"
)

// IRODSTicketRestrictions contains irods ticket restriction information
type IRODSTicketRestrictions struct {
	// AllowedHosts is a list of allowed hosts
	AllowedHosts []string `json:"allowed_hosts"`
	// AllowedUserName is a list of allowed user names
	AllowedUserNames []string `json:"allowed_user_names"`
	// AllowedGroupNames is a list of allowed group names
	AllowedGroupNames []string `json:"allowed_group_names"`
}

// ToString stringifies the object
func (ticket *IRODSTicketRestrictions) ToString() string {
	return fmt.Sprintf("<IRODSTicketRestrictions %v %v %v>", ticket.AllowedHosts, ticket.AllowedUserNames, ticket.AllowedGroupNames)
}
