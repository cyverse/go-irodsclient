package util

import (
	"fmt"
	"net"
)

// ReadBytes reads data from socket in a particular size
func ReadBytes(socket net.Conn, buffer []byte, size int) (int, error) {
	sizeLeft := size
	actualRead := 0

	for sizeLeft > 0 {
		sizeRead, err := socket.Read(buffer[actualRead:size])
		if err != nil {
			return actualRead, err
		}

		sizeLeft -= sizeRead
		actualRead += sizeRead
	}

	if sizeLeft < 0 {
		return actualRead, fmt.Errorf("Read more bytes than requested - %d requested, but %d read", size, actualRead)
	}

	return actualRead, nil
}

// WriteBytes writes data to socket
func WriteBytes(socket net.Conn, buffer []byte, size int) error {
	sizeLeft := len(buffer)
	actualWrite := 0

	for sizeLeft > 0 {
		sizeWrite, err := socket.Write(buffer[actualWrite:size])
		if err != nil {
			return err
		}

		sizeLeft -= sizeWrite
		actualWrite += sizeWrite
	}

	return nil
}
