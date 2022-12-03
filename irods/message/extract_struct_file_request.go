package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageExtractStructFileRequest stores struct file extraction request
type IRODSMessageExtractStructFileRequest struct {
	XMLName          xml.Name             `xml:"StructFileExtAndRegInp_PI"`
	Path             string               `xml:"objPath"`
	TargetCollection string               `xml:"collection"`
	OperationType    int                  `xml:"oprType"`
	Flags            int                  `xml:"flags"` // unused
	KeyVals          IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageExtractStructFileRequest creates a IRODSMessageExtractstructfileRequest message
func NewIRODSMessageExtractStructFileRequest(path string, targetCollection string, resource string, dataType types.DataType, force bool) *IRODSMessageExtractStructFileRequest {
	request := &IRODSMessageExtractStructFileRequest{
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

	request.KeyVals.Add(string(common.BULK_OPR_KW), "")

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageExtractStructFileRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageExtractStructFileRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageExtractStructFileRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageExtractStructFileRequest) GetMessage() (*IRODSMessage, error) {
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
