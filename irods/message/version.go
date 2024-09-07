package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
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
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageVersion) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
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

// GetMessage builds a message
func (msg *IRODSMessageVersion) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageVersion) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return xerrors.Errorf("failed to get irods message from message body: %w", err)
	}
	return nil
}

func (msg *IRODSMessageVersion) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
