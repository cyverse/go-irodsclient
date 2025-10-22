package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
)

// IRODSMessageInt stores int message
type IRODSMessageInt struct {
	XMLName xml.Name `xml:"INT_PI"`
	Value   int      `xml:"myInt"`
}

// NewIRODSMessageInt creates a IRODSMessageInt message
func NewIRODSMessageInt(intValue int) (*IRODSMessageInt, error) {
	return &IRODSMessageInt{
		Value: intValue,
	}, nil
}

// GetBytes returns byte array
func (msg *IRODSMessageInt) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageInt) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}
