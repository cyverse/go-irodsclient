package util

import (
	"net"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

// ReadBytes reads data from socket in a particular size
func ReadBytes(socket net.Conn, buffer []byte, size int) (int, error) {
	readLen, err := ReadBytesWithTrackerCallBack(socket, buffer, size, nil)
	if err != nil {
		return readLen, xerrors.Errorf("failed to read bytes from socket: %w", err)
	}
	return readLen, nil
}

// ReadBytesWithTrackerCallBack reads data from socket in a particular size
func ReadBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) (int, error) {
	totalSizeToRead := size
	sizeLeft := size
	actualRead := 0

	for sizeLeft > 0 {
		sizeRead, err := socket.Read(buffer[actualRead:size])
		if err != nil {
			return actualRead, xerrors.Errorf("failed to read from socket: %w", err)
		}

		sizeLeft -= sizeRead
		actualRead += sizeRead

		if callback != nil {
			callback(int64(actualRead), int64(totalSizeToRead))
		}
	}

	if sizeLeft < 0 {
		return actualRead, xerrors.Errorf("received more bytes than requested, %d requested, but %d read", size, actualRead)
	}

	return actualRead, nil
}

// WriteBytes writes data to socket
func WriteBytes(socket net.Conn, buffer []byte, size int) error {
	err := WriteBytesWithTrackerCallBack(socket, buffer, size, nil)
	if err != nil {
		return xerrors.Errorf("failed to write bytes to socket: %w", err)
	}
	return nil
}

// WriteBytesWithTrackerCallBack writes data to socket
func WriteBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) error {
	totalSizeToSend := size
	sizeLeft := size
	actualWrite := 0

	for sizeLeft > 0 {
		sizeWrite, err := socket.Write(buffer[actualWrite:size])
		if err != nil {
			return xerrors.Errorf("failed to write to socket: %w", err)
		}

		sizeLeft -= sizeWrite
		actualWrite += sizeWrite

		if callback != nil {
			callback(int64(actualWrite), int64(totalSizeToSend))
		}
	}

	return nil
}
