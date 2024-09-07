package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

const (
	// RODS_MESSAGE_CONNECT_TYPE is a message type for establishing a new connection
	RODS_MESSAGE_CONNECT_TYPE MessageType = "RODS_CONNECT"
	// RequestNegotiationOptionString is an option string for requesting server negotiation
	RequestNegotiationOptionString string = "request_server_negotiation"
)

// IRODSMessageStartupPack stores startup message
type IRODSMessageStartupPack struct {
	XMLName         xml.Name `xml:"StartupPack_PI"`
	Protocol        int      `xml:"irodsProt"`
	ReconnectFlag   int      `xml:"reconnFlag"`
	ConnectionCount int      `xml:"connectCnt"`
	ProxyUser       string   `xml:"proxyUser"`
	ProxyRcatZone   string   `xml:"proxyRcatZone"`
	ClientUser      string   `xml:"clientUser"`
	ClientRcatZone  string   `xml:"clientRcatZone"`
	ReleaseVersion  string   `xml:"relVersion"`
	APIVersion      string   `xml:"apiVersion"`
	Option          string   `xml:"option"`
}

// NewIRODSMessageStartupPack creates a IRODSMessageStartupPack message
func NewIRODSMessageStartupPack(account *types.IRODSAccount, option string, requireNegotiation bool) *IRODSMessageStartupPack {
	optionString := option
	if requireNegotiation {
		// append a flag
		optionString = fmt.Sprintf("%s;%s", optionString, RequestNegotiationOptionString)
	}

	return &IRODSMessageStartupPack{
		Protocol:        1,
		ReleaseVersion:  fmt.Sprintf("rods%s", common.IRODSVersionRelease),
		APIVersion:      common.IRODSVersionAPI,
		ConnectionCount: 0,
		ReconnectFlag:   0,
		ProxyUser:       account.ProxyUser,
		ProxyRcatZone:   account.ProxyZone,
		ClientUser:      account.ClientUser,
		ClientRcatZone:  account.ClientZone,
		Option:          optionString,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageStartupPack) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageStartupPack) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageStartupPack) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_CONNECT_TYPE,
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
func (msg *IRODSMessageStartupPack) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return xerrors.Errorf("failed to get irods message from message body: %w", err)
	}
	return nil
}

func (msg *IRODSMessageStartupPack) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
