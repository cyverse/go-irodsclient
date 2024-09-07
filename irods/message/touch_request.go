package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageTouchRequest stores touch request
type IRODSMessageTouchRequest struct {
	Path    string                 `json:"logical_path"`
	Options map[string]interface{} `json:"options"`
}

// NewIRODSMessageTouchRequest creates a IRODSMessageTouchRequest message
func NewIRODSMessageTouchRequest(path string, noCreate bool, resource string) *IRODSMessageTouchRequest {
	return &IRODSMessageTouchRequest{
		Path:    path,
		Options: map[string]interface{}{},
	}
}

func (msg *IRODSMessageTouchRequest) SetReplicaNumber(replica int) {
	msg.Options["replica_number"] = replica
}

func (msg *IRODSMessageTouchRequest) SetSecondsSinceEpoch(seconds int) {
	msg.Options["seconds_since_epoch"] = seconds
}

func (msg *IRODSMessageTouchRequest) SetReference(reference string) {
	msg.Options["reference"] = reference
}

// GetBytes returns byte array
func (msg *IRODSMessageTouchRequest) GetBytes() ([]byte, error) {
	jsonBody, err := json.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to json: %w", err)
	}

	jsonBodyBin := base64.StdEncoding.EncodeToString(jsonBody)

	binBytesBuf := IRODSMessageBinBytesBuf{
		Length: len(jsonBody), // use original data's length
		Data:   jsonBodyBin,
	}

	xmlBytes, err := xml.Marshal(binBytesBuf)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTouchRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return xerrors.Errorf("failed to decode base64 data: %w", err)
	}

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal json to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageTouchRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.TOUCH_APN),
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageTouchRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
