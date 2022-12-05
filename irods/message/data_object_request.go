package message

import "encoding/xml"

// IRODSMessageDataObjectRequest ...
type IRODSMessageDataObjectRequest struct {
	XMLName                  xml.Name                      `xml:"DataObjInp_PI"`
	Path                     string                        `xml:"objPath"`
	CreateMode               int                           `xml:"createMode"`
	OpenFlags                int                           `xml:"openFlags"`
	Offset                   int64                         `xml:"offset"`
	Size                     int64                         `xml:"dataSize"`
	Threads                  int                           `xml:"numThreads"`
	OperationType            int                           `xml:"oprType"`
	SpecialCollectionPointer IRODSMessageSpecialCollection `xml:"SpecColl_PI"`
	KeyVals                  IRODSMessageSSKeyVal          `xml:"KeyValPair_PI"`
}

type IRODSMessageSpecialCollection struct {
	XMLName           xml.Name `xml:"SpecColl_PI"`
	CollectionClass   int      `xml:"collClass"`
	Type              int      `xml:"type"`
	Collection        string   `xml:"collection"`
	ObjectPath        string   `xml:"objPath"`
	Resource          string   `xml:"resource"`
	ResourceHierarchy string   `xml:"rescHier"`
	PhysicalPath      string   `xml:"phyPath"`
	CacheDirectory    string   `xml:"cacheDir"`
	CacheDirty        int      `xml:"cacheDirty"`
	ReplicationNumber int      `xml:"replNum"`
}
