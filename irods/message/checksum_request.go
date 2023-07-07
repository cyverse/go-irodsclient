package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
)

type ChecksumRequest IRODSMessageDataObjectRequest

func (msg *ChecksumRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.DATA_OBJ_CHKSUM_AN),
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
