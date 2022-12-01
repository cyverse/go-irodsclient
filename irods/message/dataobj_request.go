package message

import "encoding/xml"

// IRODSMessageDataObjectRequest ...
type IRODSMessageDataObjectRequest struct {
	XMLName       xml.Name             `xml:"DataObjInp_PI"`
	Path          string               `xml:"objPath"`
	CreateMode    int                  `xml:"createMode"`
	OpenFlags     int                  `xml:"openFlags"`
	Offset        int64                `xml:"offset"`
	Size          int64                `xml:"dataSize"`
	Threads       int                  `xml:"numThreads"`
	OperationType int                  `xml:"oprType"`
	SpecCollPtr   IRODSMessageSpecColl `xml:"*SpecColl_PI"`
	KeyVals       IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}

type IRODSMessageSpecColl struct {
	XMLName    xml.Name `xml:"*SpecColl_PI"`
	CollClass  int      `xml:"collClass"`
	Type       int      `xml:"type"`
	Collection string   `xml:"collection"`
	ObjPath    string   `xml:"objPath"`
	Resource   string   `xml:"resource"`
	RescHier   string   `xml:"rescHier"`
	PhyPath    string   `xml:"phyPath"`
	CacheDir   string   `xml:"cacheDir"`
	CacheDirty int      `xml:"cacheDirty"`
	ReplNum    int      `xml:"replNum"`
}
