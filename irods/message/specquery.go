package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// IRODSMessageSpecQuery stores query
type IRODSMessageSpecQuery struct {
	// str *sql; str *arg1; str *arg2; str *arg3; str *arg4; str *arg5; str *arg6; str *arg7; str *arg8; str *arg9; str *arg10; int maxRows; int continueInx; int rowOffset; int options; struct KeyValPair_PI;
	XMLName       xml.Name             `xml:"specificQueryInp_PI"`
	SQL           string               `xml:"sql"`
	Arg1          string               `xml:"arg1"`
	Arg2          string               `xml:"arg2"`
	Arg3          string               `xml:"arg3"`
	Arg4          string               `xml:"arg4"`
	Arg5          string               `xml:"arg5"`
	Arg6          string               `xml:"arg6"`
	Arg7          string               `xml:"arg7"`
	Arg8          string               `xml:"arg8"`
	Arg9          string               `xml:"arg9"`
	Arg10         string               `xml:"arg10"`
	MaxRows       int                  `xml:"maxRows"`
	ContinueIndex int                  `xml:"continueInx"` // 1 for continueing, 0 for end
	RowOffset     int                  `xml:"rowOffset"`
	Options       int                  `xml:"options"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageSpecQuery creates a IRODSMessageQuery message
func NewIRODSMessageSpecQuery(sqlQuery string, args []string, maxRows int, continueIndex int, rowOffset int, options int) *IRODSMessageSpecQuery {
	q := &IRODSMessageSpecQuery{
		SQL:           sqlQuery,
		MaxRows:       maxRows,
		ContinueIndex: continueIndex,
		RowOffset:     rowOffset,
		Options:       options,
		KeyVals: IRODSMessageSSKeyVal{
			Length: 0,
		},
	}

	argMap := []*string{&q.Arg1, &q.Arg2, &q.Arg3, &q.Arg4, &q.Arg5, &q.Arg6, &q.Arg7, &q.Arg8, &q.Arg9, &q.Arg10}
	for i, ptr := range argMap {
		if len(args) > i {
			*ptr = args[i]
		}
	}

	return q
}

// AddKeyVal adds a key-value pair
func (msg *IRODSMessageSpecQuery) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageSpecQuery) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageSpecQuery) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageSpecQuery) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.SPECIFIC_QUERY_AN),
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
func (msg *IRODSMessageSpecQuery) FromMessage(msgIn *IRODSMessage) error {
	if msgIn.Body == nil {
		return fmt.Errorf("cannot create a struct from an empty body")
	}

	err := msg.FromBytes(msgIn.Body.Message)
	return err
}
