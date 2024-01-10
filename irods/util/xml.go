package util

import (
	"bytes"
	"encoding/xml"
)

// EscapeXMLSpecialChars escape special chars for XML
func EscapeXMLSpecialChars(in string) string {
	var buf bytes.Buffer
	err := xml.EscapeText(&buf, []byte(in))
	if err != nil {
		return in
	}

	return buf.String()
}
