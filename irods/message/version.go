package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

const (
	// RODS_MESSAGE_VERSION_TYPE is a message type for version
	RODS_MESSAGE_VERSION_TYPE MessageType = "RODS_VERSION"
)

// IRODSMessageVersion stores version message
type IRODSMessageVersion struct {
	XMLName        xml.Name `xml:"Version_PI"`
	Status         int      `xml:"status"`
	ReleaseVersion string   `xml:"relVersion"`
	APIVersion     string   `xml:"apiVersion"`
	ReconnectPort  int      `xml:"reconnPort"`
	ReconnectAddr  string   `xml:"reconnAddr"`
	Cookie         int      `xml:"cookie"`
}

// GetBytes returns byte array
func (msg *IRODSMessageVersion) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageVersion) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageVersion) CheckError() error {
	if msg.Status < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Status))
	}
	return nil
}

// GetVersion creates IRODSVersion
func (msg *IRODSMessageVersion) GetVersion() *types.IRODSVersion {
	return &types.IRODSVersion{
		ReleaseVersion: msg.ReleaseVersion,
		APIVersion:     msg.APIVersion,
		ReconnectPort:  msg.ReconnectPort,
		ReconnectAddr:  msg.ReconnectAddr,
		Cookie:         msg.Cookie,
	}
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageVersion) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageVersion) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_VERSION_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
