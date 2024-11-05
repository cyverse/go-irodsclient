package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageCreateDataObjectRequest stores data object creation request
type IRODSMessageCreateDataObjectRequest IRODSMessageDataObjectRequest

// NewIRODSMessageCreateDataObjectRequest creates a IRODSMessageCreateDataObjectRequest message
func NewIRODSMessageCreateDataObjectRequest(path string, resource string, mode types.FileOpenMode, force bool) *IRODSMessageCreateDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageCreateDataObjectRequest{
		Path:          path,
		CreateMode:    0, //0644,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	request.KeyVals.Add(string(common.DATA_TYPE_KW), string(types.GENERIC_DT))

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	return request
}

// NewIRODSMessageCreateobjRequestWithKeyVals creates a IRODSMessageCreateobjRequest message with given keyvals
func NewIRODSMessageCreateobjRequestWithKeyVals(path string, resource string, mode types.FileOpenMode, force bool, keyvals map[string]string) *IRODSMessageCreateDataObjectRequest {
	flag := mode.GetFlag()
	request := &IRODSMessageCreateDataObjectRequest{
		Path:          path,
		CreateMode:    0, //0644,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	// if data type is not set
	if _, ok := keyvals[string(common.DATA_TYPE_KW)]; !ok {
		request.KeyVals.Add(string(common.DATA_TYPE_KW), string(types.GENERIC_DT))
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	if force {
		request.KeyVals.Add(string(common.FORCE_FLAG_KW), "")
	}

	for key, val := range keyvals {
		request.KeyVals.Add(key, val)
	}

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageCreateDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageCreateDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageCreateDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageCreateDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_CREATE_AN),
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
func (msg *IRODSMessageCreateDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
