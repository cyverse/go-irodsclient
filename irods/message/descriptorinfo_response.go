package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageDescriptorInfoResponse stores data object descriptor info. response
type IRODSMessageDescriptorInfoResponse struct {
	L3DescriptorIndex       int                    `json:"l3descInx"`
	InUseFlag               bool                   `json:"in_use"`
	OperationType           int                    `json:"operation_type"`
	OpenType                int                    `json:"open_type"`
	OperationStatus         int                    `json:"operation_status"`
	ReplicationFlag         int                    `json:"data_object_input_replica_flag"`
	DataObjectInput         map[string]interface{} `json:"data_object_input"`
	DataObjectInfo          map[string]interface{} `json:"data_object_info"`
	OtherDataObjectInfo     map[string]interface{} `json:"other_data_object_info"`
	CopiesNeeded            int                    `json:"copies_needed"`
	BytesWritten            int64                  `json:"bytes_written"`
	DataSize                int64                  `json:"data_size"`
	ReplicaStatus           int                    `json:"replica_status"`
	ChecksumFlag            int                    `json:"checksum_flag"`
	SourceL1DescriptorIndex int                    `json:"source_l1_descriptor_index"`
	Checksum                string                 `json:"checksum"`
	RemoteL1DescriptorIndex int                    `json:"remote_l1_descriptor_index"`
	StageFlag               int                    `json:"stage_flag"`
	PurgeCacheFlag          int                    `json:"purge_cache_flag"`
	LockFileDescriptor      int                    `json:"lock_file_descriptor"`
	PluginData              map[string]interface{} `json:"plugin_data"`
	ReplicaDataObjectInfo   map[string]interface{} `json:"replication_data_object_info"`
	RemoteZoneHost          map[string]interface{} `json:"remote_zone_host"`
	InPDMO                  string                 `json:"in_pdmo"`
	ReplicaToken            string                 `json:"replica_token"`
	ResourceHierarchy       string                 `json:"resource_hierarchy"`

	// stores error return
	Result int `json:"-"`
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageDescriptorInfoResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageDescriptorInfoResponse) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return err
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return err
	}

	// remove trail \x00
	actualLen := len(jsonBody)
	for i := len(jsonBody) - 1; i >= 0; i-- {
		if jsonBody[i] == '\x00' {
			actualLen = i
		}
	}
	jsonBody = jsonBody[:actualLen]

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return err
	}

	// handle fields buried in other structs
	// ResourceHierarchy
	if msg.DataObjectInfo != nil {
		if resourceHierarchy, ok := msg.DataObjectInfo["resource_hierarchy"]; ok {
			msg.ResourceHierarchy = resourceHierarchy.(string)
		}
	}

	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageDescriptorInfoResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	msg.Result = int(msgIn.Body.IntInfo)
	return err
}
