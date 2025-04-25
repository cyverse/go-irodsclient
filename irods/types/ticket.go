package types

import (
	"fmt"
	"time"
)

// TicketType determines ticket access type
type TicketType string

const (
	// TicketTypeRead is for read
	TicketTypeRead TicketType = "read"
	// TicketTypeWrite is for write
	TicketTypeWrite TicketType = "write"
)

// IRODSTicket contains irods ticket information
type IRODSTicket struct {
	ID int64 `json:"id"`
	// Name is ticket string
	Name string `json:"name"`
	// Type is access type
	Type TicketType `json:"type"`
	// Owner has the owner's name
	Owner string `json:"owner"`
	// OwnerZone has the owner's zone
	OwnerZone string `json:"owner_zone"`
	// ObjectType is type of object
	ObjectType ObjectType `json:"object_type"`
	// Path is path to the object
	Path string `json:"path"`
	// ExpirationTime is time that the ticket expires
	ExpirationTime time.Time `json:"expiration_time"`
	// UsesLimit is an access limit
	UsesLimit int64 `json:"uses_limit"`
	// UsesCount is an access count
	UsesCount int64 `json:"uses_count"`
	// WriteFileLimit is a write file limit
	WriteFileLimit int64 `json:"write_file_limit"`
	// WriteFileCount is a write file count
	WriteFileCount int64 `json:"write_file_count"`
	// WriteByteLimit is a write byte limit
	WriteByteLimit int64 `json:"write_byte_limit"`
	// WriteByteCount is a write byte count
	WriteByteCount int64 `json:"write_byte_count"`
}

// IsReadWrite returns true if the ticket is TicketTypeWrite
func (ticket *IRODSTicket) IsReadWrite() bool {
	return ticket.Type == TicketTypeWrite
}

// ToString stringifies the object
func (ticket *IRODSTicket) ToString() string {
	return fmt.Sprintf("<IRODSTicket %d %s %s %s %s>", ticket.ID, ticket.Name, ticket.Owner, ticket.OwnerZone, ticket.Path)
}

// IRODSTicketForAnonymousAccess contains minimal irods ticket information for anonymous access
type IRODSTicketForAnonymousAccess struct {
	ID int64 `json:"id"`
	// Name is ticket string
	Name string `json:"name"`
	// Type is access type
	Type TicketType `json:"type"`
	// Path is path to the object
	Path string `json:"path"`
	// ExpirationTime is time that the ticket expires
	ExpirationTime time.Time `json:"expiration_time"`
}

// ToString stringifies the object
func (ticket *IRODSTicketForAnonymousAccess) ToString() string {
	return fmt.Sprintf("<IRODSTicketForAnonymousAccess %d %s %s %s %v>", ticket.ID, ticket.Name, ticket.Type, ticket.Path, ticket.ExpirationTime)
}
