package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageGetDataObjectStatResponse stores file stat request
type IRODSMessageGetDataObjectStatResponse struct {
	XMLName                  xml.Name                      `xml:"RodsObjStat_PI"`
	Size                     int64                         `xml:"objSize"`
	Type                     int                           `xml:"objType"`
	DataMode                 int                           `xml:"dataMode"`
	DataID                   string                        `xml:"dataId"`
	ChkSum                   string                        `xml:"chksum"`
	Owner                    string                        `xml:"ownerName"`
	Zone                     string                        `xml:"ownerZone"`
	CreateTime               string                        `xml:"createTime"`
	ModifyTime               string                        `xml:"modifyTime"`
	SpecialCollectionPointer IRODSMessageSpecialCollection `xml:"*SpecColl_PI"`
	// stores error return
	Result int `xml:"-"`
}

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectStatResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageGetDataObjectStatResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetDataObjectStatResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageGetDataObjectStatResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	msg.Result = int(msgIn.Body.IntInfo)
	return err
}
