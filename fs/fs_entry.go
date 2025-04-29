package fs

import (
	"fmt"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
)

// EntryType defines types of Entry
type EntryType string

const (
	// FileEntry is a Entry type for a file
	FileEntry EntryType = "file"
	// DirectoryEntry is a Entry type for a directory
	DirectoryEntry EntryType = "directory"
)

// Entry is a struct for filesystem entry
type Entry struct {
	ID                int64                   `json:"id"`
	Type              EntryType               `json:"type"`
	Name              string                  `json:"name"`
	Path              string                  `json:"path"`
	Owner             string                  `json:"owner"`
	Size              int64                   `json:"size"`
	DataType          string                  `json:"data_type"`
	CreateTime        time.Time               `json:"create_time"`
	ModifyTime        time.Time               `json:"modify_time"`
	CheckSumAlgorithm types.ChecksumAlgorithm `json:"checksum_algorithm"`
	CheckSum          []byte                  `json:"checksum"`
}

// ToString stringifies the object
func (entry *Entry) ToString() string {
	return fmt.Sprintf("<Entry %d %s %s %s %d %s %s %s>", entry.ID, entry.Type, entry.Path, entry.Owner, entry.Size, entry.DataType, entry.CreateTime, entry.ModifyTime)
}

// IsDir returns if the entry is for directory
func (entry *Entry) IsDir() bool {
	return entry.Type == DirectoryEntry
}
