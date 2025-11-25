package message

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageNewAuthPluginResponse stores new authentication plugin response
type IRODSMessageNewAuthPluginResponse struct {
	AuthContext map[string]interface{}
	Result      int
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageNewAuthPluginResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageNewAuthPluginResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Header == nil {
		return errors.Errorf("empty message header")
	}

	msg.Result = int(msgIn.Header.IntInfo)

	if msgIn.Body == nil {
		return nil
	}

	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(msgIn.Body.Message, &binBytesBuf)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}

	dataJson, err := base64.StdEncoding.DecodeString(string(binBytesBuf.Data))
	if err != nil {
		return errors.Wrapf(err, "failed to decode base64 message")
	}

	nullIndex := bytes.IndexByte(dataJson, '\x00')
	if nullIndex >= 0 {
		dataJson = dataJson[:nullIndex]
	}

	err = json.Unmarshal(dataJson, &msg.AuthContext)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal json to auth context")
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageNewAuthPluginResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
