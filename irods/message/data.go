package message

// IRODSMessageData stores byte data
type IRODSMessageData struct {
	Type MessageType
	Data []byte
}

// NewIRODSMessageData creates a IRODSMessageData message
func NewIRODSMessageData(msgType MessageType, data []byte) *IRODSMessageData {
	return &IRODSMessageData{
		Type: msgType,
		Data: data,
	}
}

// GetMessageBody builds a message body
func (msg *IRODSMessageData) GetMessageBody() (*IRODSMessageBody, error) {
	return &IRODSMessageBody{
		Type:    msg.Type,
		Message: msg.Data,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}, nil
}
