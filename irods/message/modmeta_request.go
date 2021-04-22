package message

import (
	"encoding/xml"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// IRODSMessageModMetaRequest stores alter metadata request
type IRODSMessageModMetaRequest struct {
	XMLName      xml.Name `xml:"ModAVUMetadataInp_PI"`
	Operation    string   `xml:"arg0"` // add, adda, rm, rmw, rmi, cp, mod, set
	ItemType     string   `xml:"arg1"` // -d, -D, -c, -C, -r, -R, -u, -U
	ItemName     string   `xml:"arg2"`
	AttrName     string   `xml:"arg3"`
	AttrValue    string   `xml:"arg4"`
	AttrUnits    string   `xml:"arg5"`
	NewAttrName  string   `xml:"arg6"` // new attr name (for mod)
	NewAttrValue string   `xml:"arg7"` // new attr value (for mod)
	NewAttrUnits string   `xml:"arg8"` // new attr unit (for mod)
	Arg9         string   `xml:"arg9"` // unused
}

// NewIRODSMessageAddMetadataRequest creates a IRODSMessageModMetaRequest message for adding a metadata AVU on some item.
// metadata.AVUID is ignored
func NewIRODSMessageAddMetadataRequest(itemType types.IRODSMetaItemType, itemName string, metadata *types.IRODSMeta) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation: "add",
		ItemType:  string(itemType),
		ItemName:  itemName,
		AttrName:  metadata.Name,
		AttrValue: metadata.Value,
		AttrUnits: metadata.Units,
	}

	return request
}

// NewIRODSMessageReplaceMetadataRequest creates a IRODSMessageModMetaRequest message for replacing a metadata AVU.
// oldMetadata.AVUID and newMetadata.AVUID are ignored, the old AVU is queried by its name, value and unit.
func NewIRODSMessageReplaceMetadataRequest(itemType types.IRODSMetaItemType, itemName string, oldMetadata *types.IRODSMeta, newMetadata *types.IRODSMeta) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation:    "mod",
		ItemType:     string(itemType),
		ItemName:     itemName,
		AttrName:     oldMetadata.Name,
		AttrValue:    oldMetadata.Value,
		AttrUnits:    oldMetadata.Units,
		NewAttrName:  newMetadata.Name,
		NewAttrValue: newMetadata.Value,
		NewAttrUnits: newMetadata.Units,
	}

	return request
}

// NewIRODSMessageRemoveMetadataRequest creates a IRODSMessageModMetaRequest message for removing a metadata AVU.
// metadata.AVUID is ignored, the AVU is queried by its name, value and unit.
func NewIRODSMessageRemoveMetadataRequest(itemType types.IRODSMetaItemType, itemName string, metadata *types.IRODSMeta) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation: "rm",
		ItemType:  string(itemType),
		ItemName:  itemName,
		AttrName:  metadata.Name,
		AttrValue: metadata.Value,
		AttrUnits: metadata.Units,
	}

	return request
}

// NewIRODSMessageRemoveMetadataByIDRequest creates a IRODSMessageModMetaRequest message for removing a metadata AVU by its AVUID.
func NewIRODSMessageRemoveMetadataByIDRequest(itemType types.IRODSMetaItemType, itemName string, AVUID int64) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation: "rmi",
		ItemType:  string(itemType),
		ItemName:  itemName,
		AttrName:  fmt.Sprintf("%d", AVUID),
	}

	return request
}

// NewIRODSMessageRemoveMetadataWildcardRequest creates a IRODSMessageModMetaRequest message for removing a metadata AVU by itemName and attributeValue.
func NewIRODSMessageRemoveMetadataWildcardRequest(itemType types.IRODSMetaItemType, itemName, attName string) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation: "rmw",
		ItemType:  string(itemType),
		ItemName:  itemName,
		AttrName:  attName,
		AttrValue: "%",
		AttrUnits: "%",
	}

	return request
}

// NewIRODSMessageSetMetadataRequest creates a IRODSMessageModMetaRequest message for changing the first metadata AVU on the given item with a matching attribute name to the given value an units.
// metadata.AVUID is ignored.
func NewIRODSMessageSetMetadataRequest(itemType types.IRODSMetaItemType, itemName string, metadata *types.IRODSMeta) *IRODSMessageModMetaRequest {
	request := &IRODSMessageModMetaRequest{
		Operation: "set",
		ItemType:  string(itemType),
		ItemName:  itemName,
		AttrName:  metadata.Name,
		AttrValue: metadata.Value,
		AttrUnits: metadata.Units,
	}

	return request
}

// GetBytes returns byte array
func (msg *IRODSMessageModMetaRequest) GetBytes() ([]byte, error) {
	xmlBytes, err := xml.Marshal(msg)
	return xmlBytes, err
}

// FromBytes returns struct from bytes
func (msg *IRODSMessageModMetaRequest) FromBytes(bytes []byte) error {
	err := xml.Unmarshal(bytes, msg)
	return err
}

// GetMessage builds a message
func (msg *IRODSMessageModMetaRequest) GetMessage() (*IRODSMessage, error) {
	bytes, err := msg.GetBytes()
	if err != nil {
		return nil, err
	}

	msgBody := IRODSMessageBody{
		Type:    RODS_MESSAGE_API_REQ_TYPE,
		Message: bytes,
		Error:   nil,
		Bs:      nil,
		IntInfo: int32(common.MOD_AVU_METADATA_AN),
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
