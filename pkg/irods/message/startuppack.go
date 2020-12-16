package message

import (
	"encoding/xml"
	"fmt"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

// IRODSMessageStartupPack stores startup message
type IRODSMessageStartupPack struct {
	XMLName         xml.Name `xml:"StartupPack_PI"`
	Protocol        int      `xml:"irodsProt"`
	ReleaseVersion  string   `xml:"relVersion"`
	APIVersion      string   `xml:"apiVersion"`
	ConnectionCount int      `xml:"connectCnt"`
	ReconnectFlag   int      `xml:"reconnFlag"`
	ProxyUser       string   `xml:"proxyUser"`
	ProxyRcatZone   string   `xml:"proxyRcatZone"`
	ClientUser      string   `xml:"clientUser"`
	ClientRcatZone  string   `xml:"clientRcatZone"`
	Option          string   `xml:"option"`
}

// NewIRODSMessageStartupPack creates a IRODSMessageStartupPack message
func NewIRODSMessageStartupPack(account *types.IRODSAccount) *IRODSMessageStartupPack {
	return NewIRODSMessageStartupPackWithOption(account, "")
}

// NewIRODSMessageStartupPackWithOption creates a IRODSMessageStartupPack message
func NewIRODSMessageStartupPackWithOption(account *types.IRODSAccount, option string) *IRODSMessageStartupPack {
	startupPack := &IRODSMessageStartupPack{
		Protocol:        1,
		ReleaseVersion:  fmt.Sprintf("rods%s", common.IRODS_REL_VERSION),
		APIVersion:      common.IRODS_API_VERSION,
		ConnectionCount: 0,
		ReconnectFlag:   0,
		ProxyUser:       account.ProxyUser,
		ProxyRcatZone:   account.ProxyZone,
		ClientUser:      account.ClientUser,
		ClientRcatZone:  account.ClientZone,
		Option:          option,
	}

	return startupPack
}

// ToXML returns XML byte array
func (pack *IRODSMessageStartupPack) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(pack)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (pack *IRODSMessageStartupPack) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, pack)
	return err
}
