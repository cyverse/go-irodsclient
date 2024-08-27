package fs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

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
		return xerrors.Errorf("failed to create file %q: %w", status.status.StatusFilePath, err)
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
		return xerrors.Errorf("failed to delete status file %q: %w", status.status.StatusFilePath, err)
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
		return nil, xerrors.Errorf("failed to read file %q: %w", statusFilePath, err)
	}

	status, err := newDataObjectTransferFromBytes(data)
	if err != nil {
		return nil, xerrors.Errorf("failed to create transfer status for %q: %w", localPath, err)
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

		return nil, xerrors.Errorf("failed to read transfer status for %q: %w", localPath, err)
	}

	if !status.status.Validate(localPath, size) {
		// cannot reuse, create a new
		status := NewDataObjectTransferStatusLocal(localPath, size, threads)
		return status, nil
	}

	return status, nil
}
