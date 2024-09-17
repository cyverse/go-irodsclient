package message

import (
	"encoding/xml"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// IRODSMessageQuerySpecificRequest stores specific query request
type IRODSMessageQuerySpecificRequest struct {
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
	ContinueIndex int                  `xml:"continueInx"` // 1 for continuing, 0 for end
	RowOffset     int                  `xml:"rowOffset"`
	Options       int                  `xml:"options"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

// NewIRODSMessageQuerySpecificRequest creates a IRODSMessageQuerySpecificRequest message
func NewIRODSMessageQuerySpecificRequest(sqlQuery string, args []string, maxRows int, continueIndex int, rowOffset int, options int) *IRODSMessageQuerySpecificRequest {
	q := &IRODSMessageQuerySpecificRequest{
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
func (msg *IRODSMessageQuerySpecificRequest) AddKeyVal(key common.KeyWord, val string) {
	msg.KeyVals.Add(string(key), val)
}

// GetBytes returns byte array
func (msg *IRODSMessageQuerySpecificRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal irods message to xml: %w", err)
	}
	return xmlBytes, nil
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageQuerySpecificRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal xml to irods message: %w", err)
	}
	return nil
}

// GetMessage builds a message
func (msg *IRODSMessageQuerySpecificRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get bytes from irods message: %w", err)
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
		return nil, xerrors.Errorf("failed to build header from irods message: %w", err)
	}

	return &IRODSMessage{
		Header: msgHeader,
		Body:   &msgBody,
	}, nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageQuerySpecificRequest) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
