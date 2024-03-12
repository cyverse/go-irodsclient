package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageGetDataObjectResponse stores file get response
type IRODSMessageGetDataObjectResponse struct {
	XMLName        xml.Name              `xml:"PortalOprOut_PI"`
	Status         int                   `xml:"status"`
	FileDescriptor int                   `xml:"l1descInx"`
	Threads        int                   `xml:"numThreads"`
	CheckSum       string                `xml:"chksum"`
	PortList       *IRODSMessagePortList `xml:"PortList_PI"`
	// stores error return
	// error if result < 0
	// data is included if result == 0
	// any value >= 0 is fine
	Result int `xml:"-"`
}

type IRODSMessagePortList struct {
	XMLName      xml.Name `xml:"PortList_PI"`
	Port         int      `xml:"portNum"`
	Cookie       int      `xml:"cookie"`
	ServerSocket int      `xml:"sock"` // server's sock number
	WindowSize   int      `xml:"windowSize"`
	HostAddress  string   `xml:"hostAddr"`
}

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageGetDataObjectResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDataObjectResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageGetDataObjectResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return xerrors.Errorf("failed to get irods message from message body")
		}
	}

	return nil
}
