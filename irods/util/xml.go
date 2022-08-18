package util

import (
	"bytes"
	"encoding/xml"
)

func EscapeXMLSpecialChars(in string) string {
	var buf bytes.Buffer
	err := xml.EscapeText(&buf, []byte(in))
	if err != nil {
		return in
	}

	return buf.String()
}
