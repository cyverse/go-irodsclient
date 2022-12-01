package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageObjStatResponse stores file stat request
type IRODSMessageObjStatResponse struct {
	XMLName     xml.Name             `xml:"RodsObjStat_PI"`
	Size        int64                `xml:"objSize"`
	Type        int                  `xml:"objType"`
	DataMode    int                  `xml:"dataMode"`
	DataID      string               `xml:"dataId"`
	ChkSum      string               `xml:"chksum"`
	Owner       string               `xml:"ownerName"`
	Zone        string               `xml:"ownerZone"`
	CreateTime  string               `xml:"createTime"`
	ModifyTime  string               `xml:"modifyTime"`
	SpecCollPtr IRODSMessageSpecColl `xml:"*SpecColl_PI"`
	// stores error return
	Result int `xml:"-"`
}

// GetBytes returns byte array
func (msg *IRODSMessageObjStatResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageObjStatResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageObjStatResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageObjStatResponse) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REPLY_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(msg.Result),
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
func (msg *IRODSMessageObjStatResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	msg.Result = int(msgIn.Body.IntInfo)
	return err
}
