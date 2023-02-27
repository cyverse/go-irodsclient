package connection

import (
	"bytes"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cyverse/go-irodsclient/irods/message"
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

func (conn *IRODSConnection) talksCorrectXML() bool {
	if conn.serverVersion == nil {
		// We don't know the server version yet, assume the best
		return true
	}

	if !strings.HasPrefix(conn.serverVersion.ReleaseVersion, "rods") {
		// Strange, but hopefully it talks correct xml
		return true
	}

	version := strings.Split(conn.serverVersion.ReleaseVersion[4:], ".")

	if len(version) != 3 {
		// Strange, but hopefully it talks correct xml
		return true
	}

	major, _ := strconv.Atoi(version[0])
	minor, _ := strconv.Atoi(version[1])
	release, _ := strconv.Atoi(version[2])

	return major > 4 || (major == 4 && minor > 2) || (major == 4 && minor == 2 && release > 8)
}

// PostprocessMessage prepares a message that is received from irods for XML parsing.
func (conn *IRODSConnection) PostprocessMessage(msg *message.IRODSMessage) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error

	msg.Body.Message, err = conn.PostprocessXML(msg.Body.Message)
	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// PostprocessXML translates IRODS XML into valid XML.
// We fix the invalid encoding of ` as &quot.
func (conn *IRODSConnection) PostprocessXML(in []byte) ([]byte, error) {
	buf := in
	out := &bytes.Buffer{}

	for len(buf) > 0 {
		switch {
		// turn &quot; into `
		case bytes.HasPrefix(buf, irodsEscQuot) && !conn.talksCorrectXML():
			out.WriteByte('`')
			buf = buf[len(irodsEscQuot):]
		// turn ' into &apos;
		case buf[0] == '\'' && !conn.talksCorrectXML():
			out.Write(escApos)
			buf = buf[1:]
		// check utf8 characters for validity
		default:
			r, size := utf8.DecodeRune(buf)
			if r == utf8.RuneError && size == 1 {
				return in, ErrInvalidUTF8
			}

			if isValidChar(r) {
				out.Write(buf[:size])
			} else {
				out.Write(escFFFD)
			}

			buf = buf[size:]
		}
	}

	return out.Bytes(), nil
}

// PreprocessMessage modifies a request message to use irods dialect for XML.
func (conn *IRODSConnection) PreprocessMessage(msg *message.IRODSMessage, forPassword bool) error {
	if msg.Body == nil || msg.Body.Message == nil {
		return nil
	}

	var err error

	if forPassword {
		msg.Body.Message, err = conn.PreprocessXMLForPassword(msg.Body.Message)
	} else {
		msg.Body.Message, err = conn.PreprocessXML(msg.Body.Message)
	}

	msg.Header.MessageLen = uint32(len(msg.Body.Message))

	return err
}

// PreprocessXML translates output of xml.Marshal into XML that IRODS understands.
func (conn *IRODSConnection) PreprocessXML(in []byte) ([]byte, error) {
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
			if conn.talksCorrectXML() {
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
		case buf[0] == '`' && !conn.talksCorrectXML():
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

// PreprocessXMLForPassword translates output of xml.Marshal into XML that IRODS understands.
func (conn *IRODSConnection) PreprocessXMLForPassword(in []byte) ([]byte, error) {
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

func isValidChar(r rune) bool {
	return r == 0x09 ||
		r == 0x0A ||
		r == 0x0D ||
		r >= 0x20 && r <= 0xD7FF ||
		r >= 0xE000 && r <= 0xFFFD ||
		r >= 0x10000 && r <= 0x10FFFF
}
