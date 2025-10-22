package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

type IRODSMessageQuerySpecialCollection IRODSMessageDataObjectRequest

// GetBytes returns byte array
func (msg *IRODSMessageQuerySpecialCollection) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQuerySpecialCollection) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageQuerySpecialCollection) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bytes from irods message")
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.QUERY_SPEC_COLL_AN),
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageQuerySpecialCollection) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return errors.Errorf("empty message body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	if err != nil {
		return errors.Wrapf(err, "failed to get irods message from message body")
	}
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageQuerySpecialCollection) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
