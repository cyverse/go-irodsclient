package util

import (
	"io"
	"net"
	"syscall"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

// ReadBytes reads data from socket in a particular size
func ReadBytes(socket net.Conn, buffer []byte, size int) (int, error) {
	readLen, err := ReadBytesWithTrackerCallBack(socket, buffer, size, nil)
	if err != nil {
		if err == io.EOF {
			return readLen, io.EOF
		}

		return readLen, xerrors.Errorf("failed to read bytes from socket: %w", err)
	}
	return readLen, nil
}

// WriteBytes writes data to socket
func WriteBytes(socket net.Conn, buffer []byte, size int) error {
	err := WriteBytesWithTrackerCallBack(socket, buffer, size, nil)
	if err != nil {
		return xerrors.Errorf("failed to write bytes to socket: %w", err)
	}
	return nil
}

// ReadBytesWithTrackerCallBack reads data from socket in a particular size
func ReadBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) (int, error) {
	logger := log.WithFields(log.Fields{
		"package": "util",
		"func":    "ReadBytesWithTrackerCallBack",
	})

	if size == 0 {
		return 0, nil
	}

	logger.Debugf("Reading %d bytes from socket", size)
	defer logger.Debugf("Finished reading %d bytes from socket", size)

	totalSizeToRead := size
	sizeLeft := size
	actualRead := 0

	tcpConn, ok := socket.(*net.TCPConn)
	if !ok {
		return 0, xerrors.New("connection is not *net.TCPConn")
	}

	fd, err := tcpConn.File()
	if err != nil {
		return 0, xerrors.Errorf("failed to get file descriptor: %w", err)
	}
	defer fd.Close()

	for sizeLeft > 0 {
		sizeRead, readErr := syscall.Read(int(fd.Fd()), buffer[actualRead:actualRead+sizeLeft])
		if sizeRead > 0 {
			actualRead += sizeRead
			sizeLeft -= sizeRead

			if callback != nil {
				callback(int64(actualRead), int64(totalSizeToRead))
			}
		}

		logger.Debugf("Read %d bytes from socket, left %d bytes", sizeRead, sizeLeft)

		if readErr != nil {
			if readErr == io.EOF {
				return actualRead, io.EOF
			}

			return actualRead, xerrors.Errorf("failed to read from socket: %w", readErr)
		}

		logger.Debugf("end of loop: Read %d bytes from socket, left %d bytes", actualRead, sizeLeft)
	}

	logger.Debugf("out of loop: Read %d bytes from socket, left %d bytes", actualRead, sizeLeft)

	if sizeLeft < 0 {
		return actualRead, xerrors.Errorf("received more bytes than requested, %d requested, but %d read", size, actualRead)
	}

	return actualRead, nil
}

// WriteBytesWithTrackerCallBack writes data to socket
func WriteBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package": "util",
		"func":    "WriteBytesWithTrackerCallBack",
	})

	if size == 0 {
		return nil
	}

	totalSizeToSend := size
	sizeLeft := size
	actualWrite := 0

	logger.Debugf("Writing %d bytes to socket", size)
	defer logger.Debugf("Finished writing %d bytes to socket", size)

	tcpConn, ok := socket.(*net.TCPConn)
	if !ok {
		return xerrors.New("connection is not *net.TCPConn")
	}

	fd, err := tcpConn.File()
	if err != nil {
		return xerrors.Errorf("failed to get file descriptor: %w", err)
	}
	defer fd.Close()

	for sizeLeft > 0 {
		sizeWrite, writeErr := syscall.Write(int(fd.Fd()), buffer[actualWrite:actualWrite+sizeLeft])
		if sizeWrite > 0 {
			actualWrite += sizeWrite
			sizeLeft -= sizeWrite

			if callback != nil {
				callback(int64(actualWrite), int64(totalSizeToSend))
			}
		}

		logger.Debugf("Wrote %d bytes to socket, left %d bytes", sizeWrite, sizeLeft)

		if writeErr != nil {
			return xerrors.Errorf("failed to write to socket: %w", writeErr)
		}

		logger.Debugf("end of loop: Wrote %d bytes from socket, left %d bytes", actualWrite, sizeLeft)
	}

	logger.Debugf("out of loop: Wrote %d bytes from socket, left %d bytes", actualWrite, sizeLeft)

	return nil
}
