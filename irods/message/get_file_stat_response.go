package message

import (
	"encoding/xml"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageGetFileStatResponse stores data object read response
type IRODSMessageGetFileStatResponse struct {
	XMLName    xml.Name `xml:"RODS_STAT_T_PI"`
	Size       int64    `xml:"st_size"`
	Dev        int      `xml:"st_dev"`
	Ino        int      `xml:"st_ino"`
	Mode       int      `xml:"st_mode"`
	Links      int      `xml:"st_nlink"`
	UID        int      `xml:"st_uid"`
	GID        int      `xml:"st_gid"`
	Rdev       int      `xml:"st_rdev"`
	AccessTime int      `xml:"st_atim"`
	ModifyTime int      `xml:"st_mtim"`
	ChangeTime int      `xml:"st_ctim"`
	BlkSize    int      `xml:"st_blksize"`
	Blocks     int      `xml:"st_blocks"`

	// stores error return
	Result int `xml:"-"`
}

// GetBytes returns byte array
func (msg *IRODSMessageGetFileStatResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal irods message to xml")
	}
	return xmlBytes, nil
}

// CheckError returns error if server returned an error
func (msg *IRODSMessageGetFileStatResponse) CheckError() error {
	if msg.Result < 0 {
		return types.NewIRODSError(common.ErrorCode(msg.Result))
	}
	return nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageGetFileStatResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal xml to irods message")
	}
	return nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageGetFileStatResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return errors.Errorf("empty message body")
	}

	msg.Result = int(msgIn.Body.IntInfo)

	if msgIn.Body.Message != nil {
		err := msg.FromBytes(msgIn.Body.Message)
		if err != nil {
			return errors.Wrapf(err, "failed to get irods message from message body")
		}
	}

	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageGetFileStatResponse) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForResponse()
}
