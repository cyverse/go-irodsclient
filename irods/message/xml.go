package message

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"golang.org/x/xerrors"
)

var (
	// escapes from xml.Encode
	escQuot = []byte("&#34;") // shorter than "&quot;", \"
	escApos = []byte("&#39;") // shorter than "&apos;", \'
	escTab  = []byte("&#x9;")
	escNL   = []byte("&#xA;")
	escCR   = []byte("&#xD;")
	escFFFD = []byte("\uFFFD") // Unicode replacement character

	// escapes for irods
	irodsEscQuot = []byte("&quot;")
	irodsEscApos = []byte("&apos;")
)

// ErrInvalidUTF8 is returned if an invalid utf-8 character is found.
var ErrInvalidUTF8 = xerrors.Errorf("invalid utf-8 character")

// XMLCorrector is a function that corrects XML
type XMLCorrector func(msg *IRODSMessage, newXML bool) error

// GetXMLCorrectorForRequest returns a corrector for general xml request
func GetXMLCorrectorForRequest() XMLCorrector {
	return CorrectXMLRequestMessage
}

// GetXMLCorrectorForPasswordRequest returns a corrector for password xml request
func GetXMLCorrectorForPasswordRequest() XMLCorrector {
	return CorrectXMLRequestMessageForPassword
}

// GetXMLCorrectorForResponse returns a corrector for general xml response
func GetXMLCorrectorForResponse() XMLCorrector {
	return CorrectXMLResponseMessage
}

func GetXMLCorrectorForPasswordResponse() XMLCorrector {
	return CorrectXMLResponseMessageForPassword
}

// CorrectXMLRequestMessage modifies a request message to use irods dialect for XML.
func CorrectXMLRequestMessage(msg *IRODSMessage, newXML bool) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error
	msg.Body.Message, err = correctXMLRequest(msg.Body.Message, newXML)

	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// CorrectXMLRequestMessageForPassword modifies a request message to use irods dialect for XML.
func CorrectXMLRequestMessageForPassword(msg *IRODSMessage, newXML bool) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error
	msg.Body.Message, err = correctXMLRequestForPassword(msg.Body.Message)

	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// CorrectXMLResponseMessage prepares a message that is received from irods for XML parsing.
func CorrectXMLResponseMessage(msg *IRODSMessage, newXML bool) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error

	msg.Body.Message, err = correctXMLResponse(msg.Body.Message, newXML)
	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// CorrectXMLResponseMessageForPassword prepares a message that is received from irods for XML parsing.
func CorrectXMLResponseMessageForPassword(msg *IRODSMessage, newXML bool) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error

	msg.Body.Message, err = correctXMLResponseForPassword(msg.Body.Message, newXML)
	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// correctXMLRequest translates output of xml.Marshal into XML that IRODS understands.
func correctXMLRequest(in []byte, newXML bool) ([]byte, error) {
	buf := in
	out := &bytes.Buffer{}

	for len(buf) > 0 {
		switch {
		// turn &#34; into &quot;
		case bytes.HasPrefix(buf, escQuot):
			out.Write(irodsEscQuot)
			buf = buf[len(escQuot):]
		// turn &#39 into &apos; or '
		case bytes.HasPrefix(buf, escApos):
			if newXML {
				out.Write(irodsEscApos)
			} else {
				out.WriteByte('\'')
			}
			buf = buf[len(escApos):]
		// irods does not decode encoded tabs
		case bytes.HasPrefix(buf, escTab):
			out.WriteByte('\t')
			buf = buf[len(escTab):]
		// irods does not decode encoded carriage returns
		case bytes.HasPrefix(buf, escCR):
			out.WriteByte('\r')
			buf = buf[len(escCR):]
		// irods does not decode encoded newlines
		case bytes.HasPrefix(buf, escNL):
			out.WriteByte('\n')
			buf = buf[len(escNL):]
		// turn ` into &apos;
		case buf[0] == '`' && !newXML:
			out.Write(irodsEscApos)
			buf = buf[1:]
		// pass utf8 characters
		default:
			r, size := utf8.DecodeRune(buf)
			if r == utf8.RuneError && size == 1 {
				return in, ErrInvalidUTF8
			}

			out.Write(buf[:size])
			buf = buf[size:]
		}
	}

	return out.Bytes(), nil
}

// correctXMLRequestForPassword translates output of xml.Marshal into XML that IRODS understands.
func correctXMLRequestForPassword(in []byte) ([]byte, error) {
	buf := in
	out := &bytes.Buffer{}

	for len(buf) > 0 {
		switch {
		// turn &#34; into \"
		case bytes.HasPrefix(buf, escQuot):
			out.WriteByte('"')
			buf = buf[len(escQuot):]
		// turn &#39; into \'
		case bytes.HasPrefix(buf, escApos):
			out.WriteByte('\'')
			buf = buf[len(escApos):]
		// irods does not decode encoded tabs
		case bytes.HasPrefix(buf, escTab):
			out.WriteByte('\t')
			buf = buf[len(escTab):]
		// irods does not decode encoded carriage returns
		case bytes.HasPrefix(buf, escCR):
			out.WriteByte('\r')
			buf = buf[len(escCR):]
		// irods does not decode encoded newlines
		case bytes.HasPrefix(buf, escNL):
			out.WriteByte('\n')
			buf = buf[len(escNL):]
		// pass utf8 characters
		default:
			r, size := utf8.DecodeRune(buf)
			if r == utf8.RuneError && size == 1 {
				return in, ErrInvalidUTF8
			}

			out.Write(buf[:size])
			buf = buf[size:]
		}
	}

	return out.Bytes(), nil
}

// correctXMLResponse translates IRODS XML into valid XML.
// We fix the invalid encoding of ` as &quot.
func correctXMLResponse(in []byte, newXML bool) ([]byte, error) {
	buf := in
	out := &bytes.Buffer{}

	for len(buf) > 0 {
		switch {
		// turn &quot; into `
		case bytes.HasPrefix(buf, irodsEscQuot) && !newXML:
			out.WriteByte('`')
			buf = buf[len(irodsEscQuot):]
		// turn ' into &apos;
		case buf[0] == '\'' && !newXML:
			out.Write(escApos)
			buf = buf[1:]
		// check utf8 characters for validity
		default:
			r, size := utf8.DecodeRune(buf)
			if r == utf8.RuneError && size == 1 {
				return in, ErrInvalidUTF8
			}

			if isValidUTF8(buf[:size]) {
				out.Write(buf[:size])
			} else {
				out.Write(escFFFD)
			}

			buf = buf[size:]
		}
	}

	return out.Bytes(), nil
}

// correctXMLResponseForPassword translates IRODS XML into valid XML.
// We fix the invalid encoding of ` as &quot.
func correctXMLResponseForPassword(in []byte, newXML bool) ([]byte, error) {
	//logger.Debugf("in (quoted): %q, len: %d", in, len(in))
	//logger.Debugf("in (string): %s, len: %d", in, len(in))

	buf := in
	out := &bytes.Buffer{}

	for len(buf) > 0 {
		switch {
		// turn &quot; into `
		case bytes.HasPrefix(buf, irodsEscQuot) && !newXML:
			out.WriteByte('`')
			buf = buf[len(irodsEscQuot):]
		// turn ' into &apos;
		case buf[0] == '\'' && !newXML:
			out.Write(escApos)
			buf = buf[1:]
		// check utf8 characters for validity
		default:
			if !isValidUTF8Char(buf[0]) {
				out.WriteString(fmt.Sprintf("0x%x", buf[:1]))
			} else {
				out.Write(buf[:1])
			}

			buf = buf[1:]
		}
	}

	//logger.Debugf("out (quoted): %q, len: %d", out.Bytes(), len(out.Bytes()))
	//logger.Debugf("out (string): %s, len: %d", out.Bytes(), len(out.Bytes()))

	return out.Bytes(), nil
}

func isValidUTF8(data []byte) bool {
	return utf8.Valid(data)
}

func isValidUTF8Char(ch byte) bool {
	return ch == 0x09 || // \t
		ch == 0x0A || // \n
		ch == 0x0D || // \r
		(ch >= 0x20 && ch <= 0x7E)
}
