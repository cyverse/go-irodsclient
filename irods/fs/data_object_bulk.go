package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
)

// UploadDataObject put a data object at the local path to the iRODS path
func UploadDataObject(conn *connection.IRODSConnection, localPath string, irodsPath string, resource string, replicate bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w", common.OPER_TYPE_PUT_DATA_OBJ)
	if err != nil {
		return err
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, err := f.Read(buffer)
		if err != nil {
			CloseDataObject(conn, handle)
			if err == io.EOF {
				break
			} else {
				writeErr = err
				break
			}
		}

		err = WriteDataObject(conn, handle, buffer[:bytesRead])
		if err != nil {
			CloseDataObject(conn, handle)
			writeErr = err
			break
		}
	}

	var replErr error
	// replicate
	if replicate {
		err = ReplicateDataObject(conn, irodsPath, "", true, false)
		replErr = err
	}

	if writeErr != nil {
		return writeErr
	}
	return replErr
}

// DownloadDataObject downloads a data object at the iRODS path to the local path
func DownloadDataObject(conn *connection.IRODSConnection, irodsPath string, localPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, "", "r")
	if err != nil {
		return err
	}

	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// copy
	for {
		buffer, err := ReadDataObject(conn, handle, common.ReadWriteBufferSize)
		if err != nil {
			CloseDataObject(conn, handle)
			return err
		}

		if buffer == nil || len(buffer) == 0 {
			// EOF
			CloseDataObject(conn, handle)
			return nil
		} else {
			_, err = f.Write(buffer)
			if err != nil {
				CloseDataObject(conn, handle)
				return err
			}
		}
	}
}
