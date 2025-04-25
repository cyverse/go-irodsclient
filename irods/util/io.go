package util

import (
	"io"
	"net"

	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
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
func ReadBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback types.TrackerCallBack) (int, error) {
	sizeLeft := size
	actualRead := 0

	taskInfo := types.TrackerTaskInfo{
		TaskID:          0,
		SubTaskID:       0,
		TasksTotal:      1,
		StartOffset:     0,
		Length:          int64(size),
		ProcessedLength: 0,
		Terminated:      false,
	}

	fileInfo := types.TrackerFileInfo{
		FileName:   "",
		FileLength: int64(size),
	}

	if callback != nil {
		callback(&taskInfo, &fileInfo)
	}

	defer func() {
		if callback != nil {
			taskInfo.Terminated = true

			callback(&taskInfo, &fileInfo)
		}
	}()

	for sizeLeft > 0 {
		sizeRead, err := socket.Read(buffer[actualRead:size])

		if sizeRead > 0 {
			sizeLeft -= sizeRead
			actualRead += sizeRead

			if callback != nil {
				taskInfo.ProcessedLength = int64(actualRead)

				callback(&taskInfo, &fileInfo)
			}
		}

		if err != nil {
			if err == io.EOF {
				return actualRead, io.EOF
			}

			return actualRead, xerrors.Errorf("failed to read from socket: %w", err)
		}
	}

	if sizeLeft < 0 {
		return actualRead, xerrors.Errorf("received more bytes than requested, %d requested, but %d read", size, actualRead)
	}

	return actualRead, nil
}

// WriteBytesWithTrackerCallBack writes data to socket
func WriteBytesWithTrackerCallBack(socket net.Conn, buffer []byte, size int, callback types.TrackerCallBack) error {
	sizeLeft := size
	actualWrite := 0

	taskInfo := types.TrackerTaskInfo{
		TaskID:          0,
		SubTaskID:       0,
		TasksTotal:      1,
		StartOffset:     0,
		Length:          int64(size),
		ProcessedLength: 0,
		Terminated:      false,
	}

	fileInfo := types.TrackerFileInfo{
		FileName:   "",
		FileLength: int64(size),
	}

	if callback != nil {
		callback(&taskInfo, &fileInfo)
	}

	defer func() {
		if callback != nil {
			taskInfo.Terminated = true

			callback(&taskInfo, &fileInfo)
		}
	}()

	for sizeLeft > 0 {
		sizeWrite, err := socket.Write(buffer[actualWrite:size])

		if sizeWrite > 0 {
			sizeLeft -= sizeWrite
			actualWrite += sizeWrite

			if callback != nil {
				taskInfo.ProcessedLength = int64(actualWrite)

				callback(&taskInfo, &fileInfo)
			}
		}

		if err != nil {
			return xerrors.Errorf("failed to write to socket: %w", err)
		}
	}

	return nil
}
