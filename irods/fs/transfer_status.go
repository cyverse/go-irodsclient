package fs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

const (
	DataObjectTransferStatusFilePrefix string = ".grc."
	DataObjectTransferStatusFileSuffix string = ".trx_status"
)

// DataObjectTransferStatusEntry
type DataObjectTransferStatusEntry struct {
	StartOffset     int64 `json:"start_offset"`
	Length          int64 `json:"length"`
	CompletedLength int64 `json:"completed_length"`
}

// DataObjectTransferStatus represents data object transfer
type DataObjectTransferStatus struct {
	Path           string                                   `json:"path"`
	StatusFilePath string                                   `json:"status_file_path"`
	Size           int64                                    `json:"size"`
	Threads        int                                      `json:"threads"`
	StatusMap      map[int64]*DataObjectTransferStatusEntry `json:"-"`
}

func (status *DataObjectTransferStatus) Validate(path string, size int64) bool {
	if status.Path != path {
		return false
	}

	if status.Size != size {
		return false
	}

	return true
}

// IsDataObjectTransferStatusFile checks if the file is transfer status file
func IsDataObjectTransferStatusFile(p string) bool {
	filename := util.GetBasename(p)
	return strings.HasPrefix(filename, DataObjectTransferStatusFilePrefix) && strings.HasSuffix(filename, DataObjectTransferStatusFileSuffix)
}

// GetDataObjectTransferStatusFilePath returns transfer status file path
func GetDataObjectTransferStatusFilePath(p string) string {
	dir := util.GetDir(p)
	filename := util.GetBasename(p)
	if strings.HasPrefix(filename, DataObjectTransferStatusFilePrefix) && strings.HasSuffix(filename, DataObjectTransferStatusFileSuffix) {
		// p is status file
		return p
	}

	statusFilename := fmt.Sprintf("%s%s%s", DataObjectTransferStatusFilePrefix, filename, DataObjectTransferStatusFileSuffix)
	return util.Join(dir, statusFilename)
}

// NewDataObjectTransferStatus creates new DataObjectTransferStatus
func NewDataObjectTransferStatus(path string, size int64, threads int) *DataObjectTransferStatus {
	return &DataObjectTransferStatus{
		Path:           path,
		StatusFilePath: GetDataObjectTransferStatusFilePath(path),
		Size:           size,
		Threads:        threads,
		StatusMap:      map[int64]*DataObjectTransferStatusEntry{},
	}
}

func newDataObjectTransferFromBytes(data []byte) (*DataObjectTransferStatus, error) {
	byteReader := bytes.NewReader(data)
	bufReader := bufio.NewReader(byteReader)
	line, prefix, err := bufReader.ReadLine()
	if err != nil {
		return nil, xerrors.Errorf("failed to read lines from bytedata: %w", err)
	}

	if prefix {
		return nil, xerrors.Errorf("failed to read long line from bytedata, buffer overflow")
	}

	// first line is status
	transferStatus := DataObjectTransferStatus{}
	err = json.Unmarshal(line, &transferStatus)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal json data to DataObjectTransferStatus: %w", err)
	}

	statusMap := map[int64]*DataObjectTransferStatusEntry{}

	for {
		line, prefix, err := bufReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, xerrors.Errorf("failed to read lines from bytedata: %w", err)
		}

		if prefix {
			return nil, xerrors.Errorf("failed to read long line from bytedata, buffer overflow")
		}

		if len(line) > 0 {
			statusEntry := DataObjectTransferStatusEntry{}
			err = json.Unmarshal(line, &statusEntry)
			if err != nil {
				return nil, xerrors.Errorf("failed to unmarshal json data to DataObjectTransferStatusEntry: %w", err)
			}

			// update
			statusMap[statusEntry.StartOffset] = &statusEntry
		}
	}

	transferStatus.StatusMap = statusMap
	return &transferStatus, nil
}

type DataObjectTransferStatusIRODS struct {
	status     *DataObjectTransferStatus
	session    *session.IRODSSession
	resource   string
	connection *connection.IRODSConnection
	fileHandle *types.IRODSFileHandle
}

func NewDataObjectTransferStatusIRODS(session *session.IRODSSession, irodsPath string, resource string, size int64, threads int) *DataObjectTransferStatusIRODS {
	status := NewDataObjectTransferStatus(irodsPath, size, threads)

	return &DataObjectTransferStatusIRODS{
		status:     status,
		session:    session,
		resource:   resource,
		connection: nil,
		fileHandle: nil,
	}
}

func (status *DataObjectTransferStatusIRODS) GetStatus() *DataObjectTransferStatus {
	return status.status
}

func (status *DataObjectTransferStatusIRODS) CreateStatusFile() error {
	conn, err := status.session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	if conn == nil || !conn.IsConnected() {
		status.session.ReturnConnection(conn)
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, status.status.StatusFilePath, status.resource, "w+")
	if err != nil {
		status.session.ReturnConnection(conn)
		return xerrors.Errorf("failed to open data object %s: %w", status.status.StatusFilePath, err)
	}

	status.connection = conn
	status.fileHandle = handle

	return nil
}

func (status *DataObjectTransferStatusIRODS) CloseStatusFile() error {
	var err error
	if status.connection != nil {
		if status.fileHandle != nil {
			err = CloseDataObject(status.connection, status.fileHandle)

			status.fileHandle = nil
		}

		err2 := status.session.ReturnConnection(status.connection)
		status.connection = nil

		if err == nil && err2 != nil {
			err = err2
		}
	}

	return err
}

func (status *DataObjectTransferStatusIRODS) DeleteStatusFile() error {
	conn, err := status.session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer status.session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	err = DeleteDataObject(conn, status.status.StatusFilePath, true)
	if err != nil {
		return xerrors.Errorf("failed to delete status file %s: %w", status.status.StatusFilePath, err)
	}

	return nil
}

func (status *DataObjectTransferStatusIRODS) WriteHeader() error {
	if status.connection == nil || status.fileHandle == nil {
		return xerrors.Errorf("failed to write header, connection or file handle is nil")
	}

	bytes, err := json.Marshal(status.status)
	if err != nil {
		return xerrors.Errorf("failed to marshal DataObjectTransferStatus to json: %w", err)
	}

	bytes = append(bytes, '\n')
	return WriteDataObject(status.connection, status.fileHandle, bytes)
}

func (status *DataObjectTransferStatusIRODS) WriteStatus(entry *DataObjectTransferStatusEntry) error {
	bytes, err := json.Marshal(entry)
	if err != nil {
		return xerrors.Errorf("failed to marshal DataObjectTransferStatusEntry to json: %w", err)
	}

	bytes = append(bytes, '\n')
	return WriteDataObject(status.connection, status.fileHandle, bytes)
}

// GetDataObjectTransferStatusIRODS returns DataObjectTransferStatusIRODS in iRODS
func GetDataObjectTransferStatusIRODS(session *session.IRODSSession, irodsPath string, resource string) (*DataObjectTransferStatusIRODS, error) {
	statusFilePath := GetDataObjectTransferStatusFilePath(irodsPath)

	conn, err := session.AcquireConnection()
	if err != nil {
		return nil, xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	buffer := bytes.Buffer{}

	handle, _, err := OpenDataObject(conn, statusFilePath, resource, "r")
	if err != nil {
		return nil, xerrors.Errorf("failed to open data object %s: %w", statusFilePath, err)
	}
	defer CloseDataObject(conn, handle)

	buffer2 := make([]byte, common.ReadWriteBufferSize)
	// copy
	for {
		readLen, readErr := ReadDataObject(conn, handle, buffer2)
		if readErr != nil && readErr != io.EOF {
			return nil, xerrors.Errorf("failed to read data object %s: %w", statusFilePath, readErr)
		}

		_, writeErr := buffer.Write(buffer2[:readLen])
		if writeErr != nil {
			return nil, xerrors.Errorf("failed to write to buffer: %w", writeErr)
		}

		if readErr == io.EOF {
			break
		}
	}

	status, err := newDataObjectTransferFromBytes(buffer.Bytes())
	if err != nil {
		return nil, xerrors.Errorf("failed to create transfer status for %s: %w", irodsPath, err)
	}

	return &DataObjectTransferStatusIRODS{
		status:     status,
		session:    session,
		resource:   resource,
		connection: nil,
		fileHandle: nil,
	}, nil
}

// GetOrNewDataObjectTransferStatusIRODS returns DataObjectTransferStatus in iRODS
func GetOrNewDataObjectTransferStatusIRODS(session *session.IRODSSession, irodsPath string, resource string, size int64, threads int) (*DataObjectTransferStatusIRODS, error) {
	status, err := GetDataObjectTransferStatusIRODS(session, irodsPath, resource)
	if err != nil {
		if types.IsFileNotFoundError(err) {
			// status file not found
			status := NewDataObjectTransferStatusIRODS(session, irodsPath, resource, size, threads)
			return status, nil
		}

		return nil, xerrors.Errorf("failed to read transfer status for %s: %w", irodsPath, err)
	}

	if !status.status.Validate(irodsPath, size) {
		// cannot reuse, create a new
		status := NewDataObjectTransferStatusIRODS(session, irodsPath, resource, size, threads)
		return status, nil
	}

	return status, nil
}

type DataObjectTransferStatusLocal struct {
	status     *DataObjectTransferStatus
	fileHandle *os.File
}

func NewDataObjectTransferStatusLocal(localPath string, size int64, threads int) *DataObjectTransferStatusLocal {
	status := NewDataObjectTransferStatus(localPath, size, threads)

	return &DataObjectTransferStatusLocal{
		status:     status,
		fileHandle: nil,
	}
}

func (status *DataObjectTransferStatusLocal) GetStatus() *DataObjectTransferStatus {
	return status.status
}

func (status *DataObjectTransferStatusLocal) CreateStatusFile() error {
	handle, err := os.Create(status.status.StatusFilePath)
	if err != nil {
		return xerrors.Errorf("failed to create file %s: %w", status.status.StatusFilePath, err)
	}

	status.fileHandle = handle
	return nil
}

func (status *DataObjectTransferStatusLocal) CloseStatusFile() error {
	var err error
	if status.fileHandle != nil {
		err = status.fileHandle.Close()
		status.fileHandle = nil
	}

	return err
}

func (status *DataObjectTransferStatusLocal) DeleteStatusFile() error {
	err := os.RemoveAll(status.status.StatusFilePath)
	if err != nil {
		return xerrors.Errorf("failed to delete status file %s: %w", status.status.StatusFilePath, err)
	}

	return nil
}

func (status *DataObjectTransferStatusLocal) WriteHeader() error {
	if status.fileHandle == nil {
		return xerrors.Errorf("failed to write header, file handle is nil")
	}

	bytes, err := json.Marshal(status.status)
	if err != nil {
		return xerrors.Errorf("failed to marshal DataObjectTransferStatus to json: %w", err)
	}

	bytes = append(bytes, '\n')
	_, err = status.fileHandle.Write(bytes)
	return err
}

func (status *DataObjectTransferStatusLocal) WriteStatus(entry *DataObjectTransferStatusEntry) error {
	bytes, err := json.Marshal(entry)
	if err != nil {
		return xerrors.Errorf("failed to marshal DataObjectTransferStatusEntry to json: %w", err)
	}

	bytes = append(bytes, '\n')
	_, err = status.fileHandle.Write(bytes)
	return err
}

// GetDataObjectTransferStatusLocal returns DataObjectTransferStatusLocal in local disk
func GetDataObjectTransferStatusLocal(localPath string) (*DataObjectTransferStatusLocal, error) {
	statusFilePath := GetDataObjectTransferStatusFilePath(localPath)

	_, err := os.Stat(statusFilePath)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(statusFilePath)
	if err != nil {
		return nil, xerrors.Errorf("failed to read file %s: %w", statusFilePath, err)
	}

	status, err := newDataObjectTransferFromBytes(data)
	if err != nil {
		return nil, xerrors.Errorf("failed to create transfer status for %s: %w", localPath, err)
	}

	return &DataObjectTransferStatusLocal{
		status:     status,
		fileHandle: nil,
	}, nil
}

// GetOrNewDataObjectTransferStatusLocal returns DataObjectTransferStatus in iRODS
func GetOrNewDataObjectTransferStatusLocal(localPath string, size int64, threads int) (*DataObjectTransferStatusLocal, error) {
	status, err := GetDataObjectTransferStatusLocal(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			// status file not found
			status := NewDataObjectTransferStatusLocal(localPath, size, threads)
			return status, nil
		}

		return nil, xerrors.Errorf("failed to read transfer status for %s: %w", localPath, err)
	}

	if !status.status.Validate(localPath, size) {
		// cannot reuse, create a new
		status := NewDataObjectTransferStatusLocal(localPath, size, threads)
		return status, nil
	}

	return status, nil
}
