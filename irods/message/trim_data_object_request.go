package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageTrimDataObjectRequest stores data object replication request
type IRODSMessageTrimDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageTrimDataObjectRequest creates a IRODSMessageTrimDataObjectRequest message
func NewIRODSMessageTrimDataObjectRequest(path string, resource string, minCopies int, minAgeMinutes int) *IRODSMessageTrimDataObjectRequest {
	request := &IRODSMessageTrimDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     0,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.RESC_NAME_KW), resource)
	}

	if minCopies > 0 {
		request.KeyVals.Add(string(common.COPIES_KW), fmt.Sprintf("%d", minCopies))
	}

	if minAgeMinutes > 0 {
		request.KeyVals.Add(string(common.AGE_KW), fmt.Sprintf("%d", minAgeMinutes))
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageTrimDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageTrimDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageTrimDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageTrimDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_TRIM_AN),
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
func (msg *IRODSMessageTrimDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
