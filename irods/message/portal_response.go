package message

import (
	"encoding/xml"
)

// IRODSMessagePortalResponse stores portal response
type IRODSMessagePortalResponse struct {
	XMLName        xml.Name              `xml:"PortalOprOut_PI"`
	Status         int                   `xml:"status"`
	FileDescriptor int                   `xml:"l1descInx"`
	Threads        int                   `xml:"numThreads"`
	CheckSum       string                `xml:"chksum"`
	PortList       *IRODSMessagePortList `xml:"PortList_PI"`
	// stores error return
	// error if result < 0
	// data is included if result == 0
	// any value >= 0 is fine
	Result int `xml:"-"`
}

type IRODSMessagePortList struct {
	XMLName      xml.Name `xml:"PortList_PI"`
	Port         int      `xml:"portNum"`
	Cookie       int      `xml:"cookie"`
	ServerSocket int      `xml:"sock"` // server's sock number
	WindowSize   int      `xml:"windowSize"`
	HostAddress  string   `xml:"hostAddr"`
}
