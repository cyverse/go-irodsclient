package message

import (
	"encoding/xml"
	"fmt"
)

// IRODSMessageFileStatResponse stores data object read response
type IRODSMessageFileStatResponse struct {
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
}

// GetBytes returns byte array
func (msg *IRODSMessageFileStatResponse) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageFileStatResponse) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageFileStatResponse) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("Cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
