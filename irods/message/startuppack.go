package message

import (
	"encoding/xml"
	"fmt"

	"github.com/iychoi/go-irodsclient/irods/common"
	"github.com/iychoi/go-irodsclient/irods/types"
)

const (
	RODS_MESSAGE_CONNECT_TYPE MessageType = "RODS_CONNECT"

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
	optionString := fmt.Sprintf("%s", option)
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
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageStartupPack) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageStartupPack) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
