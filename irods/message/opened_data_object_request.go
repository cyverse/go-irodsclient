package message

import "encoding/xml"

// IRODSMessageOpenedDataObjectRequest ...
type IRODSMessageOpenedDataObjectRequest struct {
	XMLName        xml.Name             `xml:"OpenedDataObjInp_PI"`
	FileDescriptor int                  `xml:"l1descInx"`
	Size           int64                `xml:"len"`
	Whence         int                  `xml:"whence"`
	OperationType  int                  `xml:"oprType"`
	Offset         int64                `xml:"offset"`
	BytesWritten   int64                `xml:"bytesWritten"`
	KeyVals        IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}
