package message

import (
	"encoding/xml"

	"golang.org/x/xerrors"
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
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageInt) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}
