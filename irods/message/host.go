package message

import (
	"encoding/xml"
	"net"
	"strconv"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageHost stores startup message
type IRODSMessageHost struct {
	XMLName  xml.Name `xml:"RHostAddr_PI"`
	Addr     string   `xml:"hostAddr"`
	Zone     string   `xml:"rodsZone"`
	Port     int      `xml:"port"`
	DummyInt int      `xml:"dummyInt"`
}

// NewIRODSMessageHost creates a IRODSMessageHost message
func NewIRODSMessageHost(resource *types.IRODSResource) (*IRODSMessageHost, error) {
	var (
		addr       string
		port       int
		portString string
		err        error
	)

	if strings.ContainsAny(resource.Location, ":") {
		addr, portString, err = net.SplitHostPort(resource.Location)
		if err != nil {
			return nil, err
		}

		port, err = strconv.Atoi(portString)
		if err != nil {
			return nil, err
		}
	} else {
		addr = resource.Location
		port = 1247
	}

	return &IRODSMessageHost{
		Addr: addr,
		Zone: resource.Zone,
		Port: port,
	}, nil
}

// GetBytes returns byte array
func (msg *IRODSMessageHost) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageHost) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}
