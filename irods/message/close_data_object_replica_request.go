package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageCloseDataObjectReplicaRequest stores data object replica close request
// Uses JSON, not XML
// Supported v4.2.9 or above
type IRODSMessageCloseDataObjectReplicaRequest struct {
	FileDescriptor            int  `json:"fd"`
	SendNotification          bool `json:"send_notification"`
	UpdateSize                bool `json:"update_size"`
	UpdateStatus              bool `json:"update_status"`
	ComputeChecksum           bool `json:"compute_checksum"`
	PreserveReplicaStateTable bool `json:"preserve_replica_state_table"`
}

// NewIRODSMessageCloseDataObjectReplicaRequest creates a IRODSMessageCloseDataObjectReplicaRequest message
func NewIRODSMessageCloseDataObjectReplicaRequest(desc int, sendNotification bool, updateSize bool, updateStatus bool, computeChecksum bool, preserveReplicaStateTable bool) *IRODSMessageCloseDataObjectReplicaRequest {
	request := &IRODSMessageCloseDataObjectReplicaRequest{
		FileDescriptor:            desc,
		SendNotification:          sendNotification,
		UpdateSize:                updateSize,
		UpdateStatus:              updateStatus,
		ComputeChecksum:           computeChecksum,
		PreserveReplicaStateTable: preserveReplicaStateTable,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageCloseDataObjectReplicaRequest) GetBytes() ([]byte, error) {
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
func (msg *IRODSMessageCloseDataObjectReplicaRequest) FromBytes(bytes []byte) error {
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
func (msg *IRODSMessageCloseDataObjectReplicaRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageCloseDataObjectReplicaRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
