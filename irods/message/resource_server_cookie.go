package message

import (
	"encoding/binary"

	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageResourceServerAuth stores resource server authentication message
type IRODSMessageResourceServerAuth struct {
	Cookie int
}

// NewIRODSMessageResourceServerAuth creates a IRODSMessageResourceServerAuth message
func NewIRODSMessageResourceServerAuth(redirectionInfo *types.IRODSRedirectionInfo) *IRODSMessageResourceServerAuth {
	return &IRODSMessageResourceServerAuth{
		Cookie: redirectionInfo.Cookie,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageResourceServerAuth) GetBytes() ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(msg.Cookie))
	return buf, nil
}
