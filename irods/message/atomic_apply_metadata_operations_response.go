package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageAtomicApplyMetadataOperationsResponse stores atomic apply metadata operations response
type IRODSMessageAtomicApplyMetadataOperationsResponse struct {
	// the server returns error details when an operation fails
	ErrorMessage   string                 `json:"error_message,omitempty"`
	OperationIndex *int                   `json:"operation_index,omitempty"`
	Operation      map[string]interface{} `json:"operation,omitempty"`

	// stores error return
	Result int `json:"-"`
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageAtomicApplyMetadataOperationsResponse) CheckError() error {
	if msg.Result < 0 {
		contextualMessage := msg.ErrorMessage
		if msg.OperationIndex != nil && *msg.OperationIndex >= 0 {
			contextualMessage = fmt.Sprintf("operation index %d: %s", *msg.OperationIndex, msg.ErrorMessage)
		}

		if len(contextualMessage) > 0 {
			return types.NewIRODSErrorWithString(common.ErrorCode(msg.Result), contextualMessage)
		}
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAtomicApplyMetadataOperationsResponse) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return errors.Wrapf(err, "failed to decode base64 data")
	}

	// remove trail \x00
	actualLen := len(jsonBody)
	for i := len(jsonBody) - 1; i >= 0; i-- {
		if jsonBody[i] == '\x00' {
			actualLen = i
		}
	}
	jsonBody = jsonBody[:actualLen]

	if len(jsonBody) == 0 {
		return nil
	}

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal json to irods message")
	}

	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAtomicApplyMetadataOperationsResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return errors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if len(msgIn.Body.Message) > 0 {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return errors.Wrapf(err, "failed to get irods message from message body")
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAtomicApplyMetadataOperationsResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
