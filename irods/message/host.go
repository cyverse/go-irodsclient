package message

import (
	"encoding/xml"
	"net"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
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
	addr := resource.Location
	port := 1247

	if strings.Contains(resource.Location, ":") {
		newAddr, portStr, err := net.SplitHostPort(resource.Location)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to split host port")
		}

		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert ascii %q to int", portStr)
		}

		addr = newAddr
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
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageHost) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}
