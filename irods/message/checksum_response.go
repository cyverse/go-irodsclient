package message

import (
	"encoding/xml"

	"golang.org/x/xerrors"
)

type ChecksumResponse struct {
	Checksum string
}

type STRI_PI struct {
	MyStr string `xml:"myStr"`
}

func (c *ChecksumResponse) FromMessage(m *IRODSMessage) error {
	if m == nil || m.Body == nil {
		return xerrors.Errorf("response message has no body")
	}
	res := STRI_PI{}
	err := xml.Unmarshal(m.Body.Message, &res)
	c.Checksum = res.MyStr
	return err
}

func (c *ChecksumResponse) CheckError() error {
	if len(c.Checksum) == 0 {
		return xerrors.Errorf("checksum not present in response message")
	}
	return nil
}
