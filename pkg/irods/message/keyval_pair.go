package message

import "encoding/xml"

// IRODSMessageRawString ...
type IRODSMessageRawString struct {
	Value string `xml:",innerxml"`
}

// IRODSMessageSSKeyVal ..
type IRODSMessageSSKeyVal struct {
	XMLName xml.Name                `xml:"KeyValPair_PI"`
	Length  int                     `xml:"ssLen"`
	Keys    []string                `xml:"keyWord,omitempty"`
	Values  []IRODSMessageRawString `xml:"svalue,omitempty"`
}

// IRODSMessageIIKeyVal ..
type IRODSMessageIIKeyVal struct {
	XMLName xml.Name `xml:"InxIvalPair_PI"`
	Length  int      `xml:"iiLen"`
	Keys    []int    `xml:"inx,omitempty"`
	Values  []int    `xml:"ivalue,omitempty"`
}

// IRODSMessageISKeyVal ..
type IRODSMessageISKeyVal struct {
	XMLName xml.Name                `xml:"InxValPair_PI"`
	Length  int                     `xml:"isLen"`
	Keys    []int                   `xml:"inx,omitempty"`
	Values  []IRODSMessageRawString `xml:"svalue,omitempty"`
}

// NewIRODSMessageSSKeyVal creates a new IRODSMessageSSKeyVal
func NewIRODSMessageSSKeyVal() *IRODSMessageSSKeyVal {
	return &IRODSMessageSSKeyVal{
		Length: 0,
	}
}

// Add adds a key-val pair
func (kv *IRODSMessageSSKeyVal) Add(key string, val string) {
	kv.Keys = append(kv.Keys, key)
	kv.Values = append(kv.Values, IRODSMessageRawString{
		Value: val,
	})
	kv.Length = len(kv.Keys)
}

// NewIRODSMessageIIKeyVal creates a new IRODSMessageIIKeyVal
func NewIRODSMessageIIKeyVal() *IRODSMessageIIKeyVal {
	return &IRODSMessageIIKeyVal{
		Length: 0,
	}
}

// Add adds a key-val pair
func (kv *IRODSMessageIIKeyVal) Add(key int, val int) {
	kv.Keys = append(kv.Keys, key)
	kv.Values = append(kv.Values, val)
	kv.Length = len(kv.Keys)
}

// NewIRODSMessageISKeyVal creates a new IRODSMessageISKeyVal
func NewIRODSMessageISKeyVal() *IRODSMessageISKeyVal {
	return &IRODSMessageISKeyVal{
		Length: 0,
	}
}

// Add adds a key-val pair
func (kv *IRODSMessageISKeyVal) Add(key int, val string) {
	kv.Keys = append(kv.Keys, key)
	kv.Values = append(kv.Values, IRODSMessageRawString{
		Value: val,
	})
	kv.Length = len(kv.Keys)
}
