package message

const (
	// RODS_MESSAGE_DISCONNECT_TYPE is a message type for disconnecting
	RODS_MESSAGE_DISCONNECT_TYPE MessageType = "RODS_DISCONNECT"
)

// IRODSMessageDisconnect stores disconnect request
type IRODSMessageDisconnect struct {
	// empty structure
}

// NewIRODSMessageDisconnect creates a IRODSMessageAuthRequest message
func NewIRODSMessageDisconnect() *IRODSMessageDisconnect {
	return &IRODSMessageDisconnect{}
}

// GetMessage builds a message
func (msg *IRODSMessageDisconnect) GetMessage() (*IRODSMessage, error) {
	msgHeader := IRODSMessageHeader{
		Type:       RODS_MESSAGE_DISCONNECT_TYPE,
		MessageLen: 0,
		ErrorLen:   0,
		BsLen:      0,
		IntInfo:    0,
	}

	return &IRODSMessage{
		Header: &msgHeader,
		Body:   nil,
	}, nil
}

// FromMessage returns struct from IRODSMessage
func (msg *IRODSMessageDisconnect) FromMessage(msgIn *IRODSMessage) error {
	return nil
}

// GetXMLCorrector returns XML corrector for this message
func (msg *IRODSMessageDisconnect) GetXMLCorrector() XMLCorrector {
	return GetXMLCorrectorForRequest()
}
