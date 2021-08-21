package message

import (
	"encoding/json"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageDescriptorInfoResponse stores data object descriptor info. response
type IRODSMessageDescriptorInfoResponse struct {
	L3DescriptorIndex       int                    `json:"l3descInx"`
	InUseFlag               int                    `json:"InuseFlag"`
	OperationType           int                    `json:"oprType"`
	OpenType                int                    `json:"openType"`
	OperationStatus         int                    `json:"oprStatus"`
	ReplicationFlag         int                    `json:"dataObjInpReplFlag"`
	DataObjectInput         map[string]interface{} `json:"dataObjInp"`
	DataObjectInfo          map[string]interface{} `json:"dataObjInfo"`
	OtherDataObjectInfo     map[string]interface{} `json:"otherDataObjInfo"`
	CopiesNeeded            int                    `json:"copiesNeeded"`
	BytesWritten            int64                  `json:"bytesWritten"`
	DataSize                int64                  `json:"dataSize"`
	ReplicaStatus           int                    `json:"replStatus"`
	ChecksumFlag            int                    `json:"chksumFlag"`
	SourceL1DescriptorIndex int                    `json:"srcL1descInx"`
	Checksum                string                 `json:"chksum"`
	RemoteL1DescriptorIndex int                    `json:"remoteL1descInx"`
	StageFlag               int                    `json:"stageFlag"`
	PurgeCacheFlag          int                    `json:"purgeCacheFlag"`
	LockFileDescriptor      int                    `json:"lockFd"`
	PluginData              map[string]interface{} `json:"pluginData"`
	ReplicaDataObjectInfo   map[string]interface{} `json:"replDataObjInfo"`
	RemoteZoneHost          map[string]interface{} `json:"remoteZoneHost"`
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
	// fill fields defined
	err := json.Unmarshal(bytes, msg)
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
