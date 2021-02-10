package message

import "encoding/xml"

// IRODSMessageFileOpenRequest ...
type IRODSMessageFileOpenRequest struct {
	XMLName       xml.Name             `xml:"DataObjInp_PI"`
	Path          string               `xml:"objPath"`
	CreateMode    int                  `xml:"createMode"`
	OpenFlags     int                  `xml:"openFlags"`
	Offset        int64                `xml:"offset"`
	Size          int64                `xml:"dataSize"`
	Threads       int                  `xml:"numThreads"`
	OperationType int                  `xml:"oprType"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}
