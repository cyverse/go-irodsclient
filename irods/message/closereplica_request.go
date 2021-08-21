package message

import (
	"encoding/json"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageClosereplicaRequest stores data object replica close request
// Uses JSON, not XML
// Supported v4.2.9 or above
type IRODSMessageClosereplicaRequest struct {
	FileDescriptor   int  `json:"fd"`
	SendNotification bool `json:"send_notification"`
	UpdateSize       bool `json:"update_size"`
	UpdateStatus     bool `json:"update_status"`
	ComputeChecksum  bool `json:"compute_checksum"`
}

// NewIRODSMessageClosereplicaRequest creates a IRODSMessageClosereplicaRequest message
func NewIRODSMessageClosereplicaRequest(desc int, sendNotification bool, updateSize bool, updateStatus bool, computeChecksum bool) *IRODSMessageClosereplicaRequest {
	request := &IRODSMessageClosereplicaRequest{
		FileDescriptor:   desc,
		SendNotification: sendNotification,
		UpdateSize:       updateSize,
		UpdateStatus:     updateStatus,
		ComputeChecksum:  computeChecksum,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageClosereplicaRequest) GetBytes() ([]byte, error) {
	jsonBytes, err := json.Marshal(msg)
	return jsonBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageClosereplicaRequest) FromBytes(bytes []byte) error {
	err := json.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageClosereplicaRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.REPLICA_CLOSE_APN),
	}

	msgHeader, err := msgBody.BuildHeader()
	if err != nil {
		return nil, err
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}
