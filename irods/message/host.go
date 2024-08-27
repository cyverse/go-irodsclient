package message

import (
	"encoding/xml"
	"net"
	"strconv"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
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
			return nil, xerrors.Errorf("failed to split host port: %w", err)
		}

		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, xerrors.Errorf("failed to convert ascii %q to int: %w", portStr, err)
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
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageHost) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}
