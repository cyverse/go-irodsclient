package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageNewAuthPluginRequest stores authentication request
type IRODSMessageNewAuthPluginRequest struct {
	AuthContext map[string]interface{}
}

// NewIRODSMessageNewAuthPluginRequest creates a IRODSMessageNewAuthPluginRequest message
func NewIRODSMessageNewAuthPluginRequest(authContext map[string]interface{}) *IRODSMessageNewAuthPluginRequest {
	return &IRODSMessageNewAuthPluginRequest{
		AuthContext: authContext,
	}
}

// GetBytes returns byte array
func (msg *IRODSMessageNewAuthPluginRequest) GetBytes() ([]byte, error) {
	jsonBody, err := json.Marshal(msg.AuthContext)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal auth context to json")
	}

	jsonBodyBin := base64.StdEncoding.EncodeToString(jsonBody)

	binBytesBuf := IRODSMessageBinBytesBuf{
		Length: len(jsonBody), // use original data's length
		Data:   jsonBodyBin,
	}

	xmlBytes, err := xml.Marshal(binBytesBuf)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageNewAuthPluginRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal irods message to xml")
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return errors.Wrapf(err, "failed to decode base64 data")
	}

	authContext := map[string]interface{}{}

	err = json.Unmarshal(jsonBody, &authContext)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal json to irods message")
	}

	msg.AuthContext = authContext
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageNewAuthPluginRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.NEW_AUTH_PLUGIN_REQ_AN),
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build header from irods message")
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

func (msg *IRODSMessageNewAuthPluginRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
