package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// IRODSMessageGetDataObjectStatResponse stores file stat response
type IRODSMessageGetDataObjectStatResponse struct {
	XMLName                  xml.Name                       `xml:"RodsObjStat_PI"`
	Size                     int64                          `xml:"objSize"`
	Type                     int                            `xml:"objType"`
	DataMode                 int                            `xml:"dataMode"`
	DataID                   string                         `xml:"dataId"`
	CheckSum                 string                         `xml:"chksum"`
	Owner                    string                         `xml:"ownerName"`
	Zone                     string                         `xml:"ownerZone"`
	CreateTime               string                         `xml:"createTime"`
	ModifyTime               string                         `xml:"modifyTime"`
	SpecialCollectionPointer *IRODSMessageSpecialCollection `xml:"SpecColl_PI"`
	// stores error return
	Result int `xml:"-"`
}

// GetBytes returns byte array
func (msg *IRODSMessageGetDataObjectStatResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
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
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageGetDataObjectStatResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return xerrors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return xerrors.Errorf("failed to get irods message from message body: %w", err)
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetDataObjectStatResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
