package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageLockDataObjectRequest stores data object lock request
type IRODSMessageLockDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageLockDataObjectRequest creates a IRODSMessageLockDataObjectRequest message
func NewIRODSMessageLockDataObjectRequest(path string, lockType types.DataObjectLockType, lockCommand types.DataObjectLockCommand) *IRODSMessageLockDataObjectRequest {
	request := &IRODSMessageLockDataObjectRequest{
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

	// lockType must be either types.DataObjectLockTypeRead or types.DataObjectLockTypeWrite

	fileOpenMode := lockType.GetFileOpenMode()
	request.OpenFlags = fileOpenMode.GetFlag()

	request.KeyVals.Add(string(common.LOCK_TYPE_KW), string(lockType))
	request.KeyVals.Add(string(common.LOCK_CMD_KW), string(lockCommand))

	return request
}

// NewIRODSMessageReadLockDataObjectRequest creates a IRODSMessageLockDataObjectRequest message
func NewIRODSMessageReadLockDataObjectRequest(path string, lockCommand types.DataObjectLockCommand) *IRODSMessageLockDataObjectRequest {
	request := &IRODSMessageLockDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     int(types.O_RDONLY),
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.KeyVals.Add(string(common.LOCK_TYPE_KW), string(types.DataObjectLockTypeRead))
	request.KeyVals.Add(string(common.LOCK_CMD_KW), string(lockCommand))

	return request
}

// NewIRODSMessageWriteLockDataObjectRequest creates a IRODSMessageLockDataObjectRequest message
func NewIRODSMessageWriteLockDataObjectRequest(path string, lockCommand types.DataObjectLockCommand) *IRODSMessageLockDataObjectRequest {
	request := &IRODSMessageLockDataObjectRequest{
		Path:          path,
		CreateMode:    0,
		OpenFlags:     int(types.O_WRONLY),
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.KeyVals.Add(string(common.LOCK_TYPE_KW), string(types.DataObjectLockTypeWrite))
	request.KeyVals.Add(string(common.LOCK_CMD_KW), string(lockCommand))

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageLockDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageLockDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageLockDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageLockDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_LOCK_AN),
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
func (msg *IRODSMessageLockDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
