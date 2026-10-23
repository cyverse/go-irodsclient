package message

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageAtomicApplyMetadataOperationsRequest stores atomic apply metadata operations request
type IRODSMessageAtomicApplyMetadataOperationsRequest struct {
	AdminMode  bool                        `json:"admin_mode"`
	EntityName string                      `json:"entity_name"`
	EntityType string                      `json:"entity_type"` // data_object, collection, resource, user
	Operations []*types.IRODSMetaOperation `json:"operations"`
}

// getAtomicMetadataEntityType returns entity type string for the given meta item type
func getAtomicMetadataEntityType(itemType types.IRODSMetaItemType) (string, error) {
	switch itemType {
	case types.IRODSDataObjectMetaItemType:
		return "data_object", nil
	case types.IRODSCollectionMetaItemType:
		return "collection", nil
	case types.IRODSResourceMetaItemType:
		return "resource", nil
	case types.IRODSUserMetaItemType:
		return "user", nil
	default:
		return "", errors.Errorf("unknown irods metadata item type %q", itemType)
	}
}

// NewIRODSMessageAtomicApplyMetadataOperationsRequest creates a IRODSMessageAtomicApplyMetadataOperationsRequest message
func NewIRODSMessageAtomicApplyMetadataOperationsRequest(itemType types.IRODSMetaItemType, itemName string, operations []*types.IRODSMetaOperation, adminMode bool) (*IRODSMessageAtomicApplyMetadataOperationsRequest, error) {
	entityType, err := getAtomicMetadataEntityType(itemType)
	if err != nil {
		return nil, err
	}

	if len(operations) == 0 {
		return nil, errors.Errorf("operations must contain one or more metadata operations")
	}

	for _, operation := range operations {
		if operation == nil {
			return nil, errors.Errorf("metadata operation must not be nil")
		}

		if operation.Operation != types.IRODSMetaOperationAdd && operation.Operation != types.IRODSMetaOperationRemove {
			return nil, errors.Errorf("unknown metadata operation %q, must be add or remove", operation.Operation)
		}
	}

	request := &IRODSMessageAtomicApplyMetadataOperationsRequest{
		AdminMode:  adminMode,
		EntityName: itemName,
		EntityType: entityType,
		Operations: operations,
	}

	return request, nil
}

// GetBytes returns byte array
func (msg *IRODSMessageAtomicApplyMetadataOperationsRequest) GetBytes() ([]byte, error) {
	jsonBody, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to json")
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
func (msg *IRODSMessageAtomicApplyMetadataOperationsRequest) FromBytes(bytes []byte) error {
	binBytesBuf := IRODSMessageBinBytesBuf{}
	err := xml.Unmarshal(bytes, &binBytesBuf)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}

	jsonBody, err := base64.StdEncoding.DecodeString(binBytesBuf.Data)
	if err != nil {
		return errors.Wrapf(err, "failed to decode base64 data")
	}

	err = json.Unmarshal(jsonBody, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal json to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageAtomicApplyMetadataOperationsRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.ATOMIC_APPLY_METADATA_OPERATIONS_APN),
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

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageAtomicApplyMetadataOperationsRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
