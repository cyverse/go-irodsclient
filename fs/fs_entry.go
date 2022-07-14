package fs

import (
	"fmt"
	"time"
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
	ID         int64
	Type       EntryType
	Name       string
	Path       string
	Owner      string
	Size       int64
	DataType   string
	CreateTime time.Time
	ModifyTime time.Time
	CheckSum   string
}

// ToString stringifies the object
func (entry *Entry) ToString() string {
	return fmt.Sprintf("<Entry %d %s %s %s %d %s %s %s>", entry.ID, entry.Type, entry.Path, entry.Owner, entry.Size, entry.DataType, entry.CreateTime, entry.ModifyTime)
}
