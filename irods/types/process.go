package types

import (
	"fmt"
	"time"
)

// IRODSProcess contains irods process information
type IRODSProcess struct {
	ID            int64     `json:"id"`
	StartTime     time.Time `json:"start_time"`
	ProxyUser     string    `json:"proxy_user"`
	ProxyZone     string    `json:"proxy_zone"`
	ClientUser    string    `json:"client_user"`
	ClientZone    string    `json:"client_zone"`
	ClientAddress string    `json:"client_address"`
	ServerAddress string    `json:"server_address"`
	ClientProgram string    `json:"client_program"`
}

// ToString stringifies the object
func (obj *IRODSProcess) ToString() string {
	return fmt.Sprintf("<IRODSProcess %d %s %s#%s %s#%s %s %s %s>", obj.ID, obj.StartTime, obj.ProxyUser, obj.ProxyZone, obj.ClientUser, obj.ClientZone, obj.ClientAddress, obj.ServerAddress, obj.ClientProgram)
}
