package connection

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"net"

	"github.com/iychoi/go-irodsclient/pkg/irods/api"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// MessageType ...
type MessageType string

const (
	RODS_EMPTY_TYPE   MessageType = ""
	RODS_CONNECT_TYPE MessageType = "RODS_CONNECT"
	RODS_VERSION_TYPE MessageType = "RODS_VERSION"
)

// IRODSMessage ...
type IRODSMessage struct {
	Type    MessageType
	Message interface{}
	Error   []byte
	Bs      []byte
	IntInfo int32
}

// NewIRODSMessage create a IRODSMessage
func NewIRODSMessage(message interface{}) *IRODSMessage {
	msgType := RODS_EMPTY_TYPE
	switch message.(type) {
	case IRODSMessageStartupPack, *IRODSMessageStartupPack:
		msgType = RODS_CONNECT_TYPE
	default:
		return nil
	}

	return &IRODSMessage{
		Type:    msgType,
		Message: message,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}
}

// Pack serializes message into bytes
func (message *IRODSMessage) Pack() ([]byte, error) {
	messageBuffer := new(bytes.Buffer)

	var bodyBytes []byte
	var err error

	messageLen := 0
	if message.Message != nil {
		if b, ok := message.Message.(IRODSMessageXMLInterface); ok {
			bodyBytes, err = b.ToXML()
			if err != nil {
				return nil, err
			}
		} else if b, ok := message.Message.([]byte); ok {
			bodyBytes = b
		} else {
			return nil, fmt.Errorf("Cannot pack unknown type - %T, %v", message.Message, message.Message)
		}
		messageLen = len(bodyBytes)
	}

	errorLen := 0
	if message.Error != nil {
		errorLen = len(message.Error)
	}
	bsLen := 0
	if message.Bs != nil {
		bsLen = len(message.Bs)
	}

	header := &IRODSMessageHeader{
		Type:       message.Type,
		MessageLen: uint32(messageLen),
		ErrorLen:   uint32(errorLen),
		BsLen:      uint32(bsLen),
		IntInfo:    message.IntInfo,
	}

	headerBytes, err := header.ToXML()
	if err != nil {
		return nil, err
	}

	// pack length - Big Endian
	headerLenBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(headerLenBuffer, uint32(len(headerBytes)))

	messageBuffer.Write(headerLenBuffer)
	messageBuffer.Write(headerBytes)
	messageBuffer.Write(bodyBytes)
	messageBuffer.Write(message.Error)
	messageBuffer.Write(message.Bs)

	return messageBuffer.Bytes(), nil
}

// IRODSMessageXMLInterface ...
type IRODSMessageXMLInterface interface {
	ToXML() ([]byte, error)
	FromXML([]byte) error
}

// IRODSMessageHeader ...
type IRODSMessageHeader struct {
	XMLName    xml.Name    `xml:"MsgHeader_PI"`
	Type       MessageType `xml:"type"`
	MessageLen uint32      `xml:"msgLen"`
	ErrorLen   uint32      `xml:"errorLen"`
	BsLen      uint32      `xml:"bsLen"`
	IntInfo    int32       `xml:"intInfo"`
}

// ToXML returns XML byte array
func (header *IRODSMessageHeader) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(header)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (header *IRODSMessageHeader) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, header)
	return err
}

// IRODSMessageStartupPack stores startup message
type IRODSMessageStartupPack struct {
	XMLName         xml.Name `xml:"StartupPack_PI"`
	Protocol        int      `xml:"irodsProt"`
	ReleaseVersion  string   `xml:"relVersion"`
	APIVersion      string   `xml:"apiVersion"`
	ConnectionCount int      `xml:"connectCnt"`
	ReconnectFlag   int      `xml:"reconnFlag"`
	ProxyUser       string   `xml:"proxyUser"`
	ProxyRcatZone   string   `xml:"proxyRcatZone"`
	ClientUser      string   `xml:"clientUser"`
	ClientRcatZone  string   `xml:"clientRcatZone"`
	Option          string   `xml:"option"`
}

// NewIRODSMessageStartupPack creates a IRODSMessageStartupPack message
func NewIRODSMessageStartupPack(account *types.IRODSAccount) *IRODSMessageStartupPack {
	return NewIRODSMessageStartupPackWithAppName(account, "")
}

// NewIRODSMessageStartupPackWithAppName creates a IRODSMessageStartupPack message
func NewIRODSMessageStartupPackWithAppName(account *types.IRODSAccount, applicationName string) *IRODSMessageStartupPack {
	startupPack := &IRODSMessageStartupPack{
		Protocol:        1,
		ReleaseVersion:  fmt.Sprintf("rods%s", api.IRODS_REL_VERSION),
		APIVersion:      api.IRODS_API_VERSION,
		ConnectionCount: 0,
		ReconnectFlag:   0,
		ProxyUser:       account.ProxyUser,
		ProxyRcatZone:   account.ProxyZone,
		ClientUser:      account.ClientUser,
		ClientRcatZone:  account.ClientZone,
		Option:          applicationName,
	}

	return startupPack
}

// ToXML returns XML byte array
func (pack *IRODSMessageStartupPack) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(pack)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (pack *IRODSMessageStartupPack) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, pack)
	return err
}

// IRODSMessageVersion stores version message
type IRODSMessageVersion struct {
	XMLName        xml.Name `xml:"Version_PI"`
	Status         int      `xml:"status"`
	ReleaseVersion string   `xml:"relVersion"`
	APIVersion     string   `xml:"apiVersion"`
	ReconnectPort  int      `xml:"reconnPort"`
	ReconnectAddr  string   `xml:"reconnectAddr"`
	Cookie         int      `xml:"cookie"`
}

// ToXML returns XML byte array
func (ver *IRODSMessageVersion) ToXML() ([]byte, error) {
	xmlBytes, err := xml.Marshal(ver)
	return xmlBytes, err
}

// FromXML returns struct from XML
func (ver *IRODSMessageVersion) FromXML(bytes []byte) error {
	err := xml.Unmarshal(bytes, ver)
	return err
}

// WriteIRODSMessage writes data to the given socket
func WriteIRODSMessage(socket net.Conn, message *IRODSMessage) error {
	util.LogDebugf("Sending a message - \n%v\n", message)

	packedMessage, err := message.Pack()
	if err != nil {
		return err
	}

	err = util.WriteBytes(socket, packedMessage)
	if err != nil {
		return err
	}

	return nil
}

// readIRODSMessageHeader reads data from the given socket and returns iRODSMessageHeader
func readIRODSMessageHeader(socket net.Conn) (*IRODSMessageHeader, error) {
	// read header size
	headerLenBuffer, err := util.ReadBytesInLen(socket, 4)
	if err != nil {
		return nil, err
	}

	headerSize := binary.BigEndian.Uint32(headerLenBuffer)
	if headerSize <= 0 {
		return nil, fmt.Errorf("Invalid header size returned - len = %d", headerSize)
	}

	// read header
	headerBuffer, err := util.ReadBytesInLen(socket, int(headerSize))
	if err != nil {
		return nil, err
	}

	header := IRODSMessageHeader{}
	err = header.FromXML(headerBuffer)
	if err != nil {
		return nil, err
	}

	return &header, nil
}

// ReadIRODSMessage reads data from the given socket and returns IRODSMessage
func ReadIRODSMessage(socket net.Conn) (*IRODSMessage, error) {
	messageHeader, err := readIRODSMessageHeader(socket)
	if err != nil {
		return nil, err
	}

	util.LogDebugf("Receiving a message header - \n%v\n", messageHeader)

	messageBytes := []byte{}
	if messageHeader.MessageLen > 0 {
		messageBytes, err = util.ReadBytesInLen(socket, int(messageHeader.MessageLen))
		if err != nil {
			return nil, err
		}
	}

	commError := []byte{}
	if messageHeader.ErrorLen > 0 {
		commError, err = util.ReadBytesInLen(socket, int(messageHeader.ErrorLen))
		if err != nil {
			return nil, err
		}
	}

	bs := []byte{}
	if messageHeader.BsLen > 0 {
		bs, err = util.ReadBytesInLen(socket, int(messageHeader.BsLen))
		if err != nil {
			return nil, err
		}
	}

	message := IRODSMessage{
		Type:    messageHeader.Type,
		Message: messageBytes,
		Error:   commError,
		Bs:      bs,
		IntInfo: messageHeader.IntInfo,
	}
	switch messageHeader.Type {
	case RODS_CONNECT_TYPE:
		message.Message = string(messageBytes)
	case RODS_VERSION_TYPE:
		messageVersion := IRODSMessageVersion{}
		err = messageVersion.FromXML(messageBytes)
		if err != nil {
			return nil, err
		}
		message.Message = messageVersion
	default:
		message.Message = messageBytes
	}

	util.LogDebugf("Receiving a message - \n%v\n", message)

	return &message, nil
}
