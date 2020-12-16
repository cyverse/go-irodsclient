package message

import (
	"encoding/xml"

	"github.com/iychoi/go-irodsclient/pkg/irods/types"
)

// IRODSMessageVersion stores version message
type IRODSMessageVersion struct {
	XMLName        xml.Name `xml:"Version_PI"`
	Status         int      `xml:"status"`
	ReleaseVersion string   `xml:"relVersion"`
	APIVersion     string   `xml:"apiVersion"`
	ReconnectPort  int      `xml:"reconnPort"`
	ReconnectAddr  string   `xml:"reconnectAddr"`
	Cookie         int      `xml:"cookie"`
}

// ToXML returns XML byte array
func (ver *IRODSMessageVersion) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(ver)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (ver *IRODSMessageVersion) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, ver)
	return err
}

// ConvertToIRODSVersion creates IRODSVersion
func (ver *IRODSMessageVersion) ConvertToIRODSVersion() *types.IRODSVersion {
	return &types.IRODSVersion{
		Status:         ver.Status,
		ReleaseVersion: ver.ReleaseVersion,
		APIVersion:     ver.APIVersion,
		ReconnectPort:  ver.ReconnectPort,
		ReconnectAddr:  ver.ReconnectAddr,
		Cookie:         ver.Cookie,
	}
}
