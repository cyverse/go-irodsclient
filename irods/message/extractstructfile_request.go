package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageExtractstructfileRequest stores struct file extraction request
type IRODSMessageExtractstructfileRequest IRODSMessageStructFileExtAndRegRequest

// NewIRODSMessageExtractstructfileRequest creates a IRODSMessageExtractstructfileRequest message
func NewIRODSMessageExtractstructfileRequest(path string, targetCollection string, resource string, dataType types.DataType, force bool) *IRODSMessageExtractstructfileRequest {
	request := &IRODSMessageExtractstructfileRequest{
		Path:             path,
		TargetCollection: targetCollection,
		OperationType:    0,
		Flags:            0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(dataType) > 0 {
		request.KeyVals.Add(string(common.DATA_TYPE_KW), string(dataType))
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageExtractstructfileRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageExtractstructfileRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageExtractstructfileRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageExtractstructfileRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.STRUCT_FILE_EXT_AND_REG_AN),
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
