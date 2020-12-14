package util

import (
	"bytes"
	"fmt"
	"net"
)

// ReadBytesInLen reads data from socket in a particular size
func ReadBytesInLen(socket net.Conn, size int) ([]byte, error) {
	messageBuffer := new(bytes.Buffer)
	sizeLeft := size
	actualRead := 0

	buffer := make([]byte, size)
	for sizeLeft > 0 {
		sizeRead, err := socket.Read(buffer[actualRead:])
		if err != nil {
			return nil, err
		}

		sizeWritten, err := messageBuffer.Write(buffer[actualRead : actualRead+sizeRead])
		if err != nil {
			return nil, err
		}

		if sizeWritten != sizeRead {
			return nil, fmt.Errorf("Could not write data into a buffer")
		}

		sizeLeft -= sizeRead
		actualRead += sizeRead
	}

	if sizeLeft < 0 {
		return nil, fmt.Errorf("Read more bytes than requested - %d requested, but %d read", size, actualRead)
	}

	return messageBuffer.Bytes(), nil
}

// WriteBytes writes data to socket
func WriteBytes(socket net.Conn, buffer []byte) error {
	sizeLeft := len(buffer)
	actualWrite := 0

	for sizeLeft > 0 {
		sizeWrite, err := socket.Write(buffer[actualWrite:])
		if err != nil {
			return err
		}

		sizeLeft -= sizeWrite
		actualWrite += sizeWrite
	}

	return nil
}
