package message

// IRODSMessageByteData stores byte data
type IRODSMessageByteData struct {
	Type MessageType
	Data []byte
}

// NewIRODSMessageByteData creates a IRODSMessageByteData message
func NewIRODSMessageByteData(msgType MessageType, data []byte) *IRODSMessageByteData {
	return &IRODSMessageByteData{
		Type: msgType,
		Data: data,
	}
}

// GetMessageBody builds a message body
func (msg *IRODSMessageByteData) GetMessageBody() (*IRODSMessageBody, error) {
	return &IRODSMessageBody{
		Type:    msg.Type,
		Message: msg.Data,
		Error:   nil,
		Bs:      nil,
		IntInfo: 0,
	}, nil
}
