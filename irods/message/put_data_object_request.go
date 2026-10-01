package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessagePutDataObjectRequest stores file put request
type IRODSMessagePutDataObjectRequest struct {
	IRODSMessageDataObjectRequest

	// Data carries the whole data object content in the message's bs section, for a put that
	// includes the data (DATA_INCLUDED_KW). It is never marshalled to xml.
	Data []byte `xml:"-"`
}

// NewIRODSMessagePutDataObjectRequest creates a IRODSMessagePutDataObjectRequest message
func NewIRODSMessagePutDataObjectRequest(path string, resource string, fileLength int64, threads int) *IRODSMessagePutDataObjectRequest {
	request := &IRODSMessagePutDataObjectRequest{
		IRODSMessageDataObjectRequest: IRODSMessageDataObjectRequest{
			Path:          path,
			CreateMode:    0,
			OpenFlags:     0,
			Offset:        0,
			Size:          fileLength,
			Threads:       threads,
			OperationType: int(common.OPER_TYPE_PUT_DATA_OBJ),
			KeyVals: IRODSMessageSSKeyVal{
				Length: 0,
			},
		},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	// the reference client sends the size as a keyword as well as in the request field,
	// as resource plugins read it from the condInput before the transfer starts
	request.AddKeyVal(common.DATA_SIZE_KW, fmt.Sprintf("%d", fileLength))

	return request
}

// NewIRODSMessagePutDataObjectRequestWithData creates a IRODSMessagePutDataObjectRequest message
// that carries the whole data object content with the request. The server stores it in one
// round trip instead of handing back a file descriptor to write to and close.
// Only use it for a data object small enough to hold in memory.
func NewIRODSMessagePutDataObjectRequestWithData(path string, resource string, force bool, data []byte) *IRODSMessagePutDataObjectRequest {
	// the server decides on the single buffer put from the keyword alone, there is no size
	// check on its side, and it answers with a status rather than a portal
	request := NewIRODSMessagePutDataObjectRequest(path, resource, int64(len(data)), 0)

	request.AddKeyVal(common.DATA_TYPE_KW, string(types.GENERIC_DT))
	request.AddKeyVal(common.DATA_INCLUDED_KW, "")

	if force {
		request.AddKeyVal(common.FORCE_FLAG_KW, "")
	}

	request.Data = data

	return request
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessagePutDataObjectRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessagePutDataObjectRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessagePutDataObjectRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessagePutDataObjectRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      msg.Data,
		IntInfo: int32(common.DATA_OBJ_PUT_AN),
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
func (msg *IRODSMessagePutDataObjectRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
