package message

import (
	"encoding/xml"
)

// IRODSMessageBinBytesBuf stores bytes buffer
type IRODSMessageBinBytesBuf struct {
	XMLName xml.Name `xml:"BinBytesBuf_PI"`

	Length int    `xml:"buflen"`
	Data   string `xml:"buf"` // data is base64 encoded

	Result int
}
