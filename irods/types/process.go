package types

import (
	"fmt"
	"time"
)

// IRODSProcess contains irods process information
type IRODSProcess struct {
	ID            int64
	StartTime     time.Time
	ProxyUser     string
	ProxyZone     string
	ClientUser    string
	ClientZone    string
	ClientAddress string
	ServerAddress string
	ClientProgram string
}

// ToString stringifies the object
func (obj *IRODSProcess) ToString() string {
	return fmt.Sprintf("<IRODSProcess %d %s %s#%s %s#%s %s %s %s>", obj.ID, obj.StartTime, obj.ProxyUser, obj.ProxyZone, obj.ClientUser, obj.ClientZone, obj.ClientAddress, obj.ServerAddress, obj.ClientProgram)
}
