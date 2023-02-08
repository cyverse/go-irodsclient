package util

import (
	"fmt"
	"net"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// ReadBytes reads data from socket in a particular size
func ReadBytes(socket net.Conn, buffer []byte, size int) (int, error) {
	return ReadBytesWithTrackerCallBack(socket, buffer, size, nil)
}

// ReadBytesWithTrackerCallBack reads data from socket in a particular size
func ReadBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) (int, error) {
	totalSizeToRead := size
	sizeLeft := size
	actualRead := 0

	for sizeLeft > 0 {
		sizeRead, err := socket.Read(buffer[actualRead:size])
		if err != nil {
			return actualRead, err
		}

		sizeLeft -= sizeRead
		actualRead += sizeRead

		if callback != nil {
			callback(int64(actualRead), int64(totalSizeToRead))
		}
	}

	if sizeLeft < 0 {
		return actualRead, fmt.Errorf("read more bytes than requested - %d requested, but %d read", size, actualRead)
	}

	return actualRead, nil
}

// WriteBytes writes data to socket
func WriteBytes(socket net.Conn, buffer []byte, size int) error {
	return WriteBytesWithTrackerCallBack(socket, buffer, size, nil)
}

// WriteBytesWithTrackerCallBack writes data to socket
func WriteBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) error {
	totalSizeToSend := len(buffer)
	sizeLeft := totalSizeToSend
	actualWrite := 0

	for sizeLeft > 0 {
		sizeWrite, err := socket.Write(buffer[actualWrite:size])
		if err != nil {
			return err
		}

		sizeLeft -= sizeWrite
		actualWrite += sizeWrite

		if callback != nil {
			callback(int64(actualWrite), int64(totalSizeToSend))
		}
	}

	return nil
}
