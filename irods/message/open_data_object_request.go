package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageOpenDataObjectRequest stores data object open request
type IRODSMessageOpenDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageOpenDataObjectRequest creates a IRODSMessageOpenDataObjectRequest message
func NewIRODSMessageOpenDataObjectRequest(path string, resource string, mode types.FileOpenMode) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	return request
}

// NewIRODSMessageOpenobjRequestWithOperation ...
func NewIRODSMessageOpenobjRequestWithOperation(path string, resource string, mode types.FileOpenMode, oper common.OperationType) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: int(oper),
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	return request
}

// NewIRODSMessageOpenobjRequestForPutParallel ...
func NewIRODSMessageOpenobjRequestForPutParallel(path string, resource string, mode types.FileOpenMode, oper common.OperationType, threadNum int, dataSize int64) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: int(oper),
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.RESC_NAME_KW), resource)
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	request.AddKeyVal(common.NUM_THREADS_KW, fmt.Sprintf("%d", threadNum))
	request.AddKeyVal(common.DATA_SIZE_KW, fmt.Sprintf("%d", dataSize))

	return request
}

// NewIRODSMessageOpenobjRequestWithReplicaToken creates a IRODSMessageOpenobjRequest message
func NewIRODSMessageOpenobjRequestWithReplicaToken(path string, mode types.FileOpenMode, resourceHierarchy string, replicaToken string, threadNum int, dataSize int64) *IRODSMessageOpenDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageOpenDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.AddKeyVal(common.RESC_HIER_STR_KW, resourceHierarchy)
	request.AddKeyVal(common.REPLICA_TOKEN_KW, replicaToken)
	request.AddKeyVal(common.NUM_THREADS_KW, fmt.Sprintf("%d", threadNum))
	request.AddKeyVal(common.DATA_SIZE_KW, fmt.Sprintf("%d", dataSize))

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageOpenDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageOpenDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageOpenDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageOpenDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_OPEN_AN),
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
func (msg *IRODSMessageOpenDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
