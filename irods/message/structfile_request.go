package message

import (
	"encoding/xml"
)

// IRODSMessageStructFileExtAndRegRequest ...
type IRODSMessageStructFileExtAndRegRequest struct {
	XMLName          xml.Name             `xml:"StructFileExtAndRegInp_PI"`
	Path             string               `xml:"objPath"`
	TargetCollection string               `xml:"collection"`
	OperationType    int                  `xml:"oprType"`
	Flags            int                  `xml:"flags"` // unused
	KeyVals          IRODSMessageSSKeyVal `xml:"KeyValPair_PI"`
}
