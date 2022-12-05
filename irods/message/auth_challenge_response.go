package message

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageAuthChallengeResponse stores auth challenge
type IRODSMessageAuthChallengeResponse struct {
	XMLName   xml.Name `xml:"authRequestOut_PI"`
	Challenge string   `xml:"challenge"`
}

// GetBytes returns byte array
func (msg *IRODSMessageAuthChallengeResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageAuthChallengeResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageAuthChallengeResponse) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REPLY_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.AUTH_REQUEST_AN),
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

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageAuthChallengeResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}

// GetChallenge returns challenge bytes
func (msg *IRODSMessageAuthChallengeResponse) GetChallenge() ([]byte, error) {
	challengeBytes, err := base64.StdEncoding.DecodeString(msg.Challenge)
	if err != nil {
		return nil, fmt.Errorf("could not decode an authentication challenge")
	}

	return challengeBytes, nil
}
