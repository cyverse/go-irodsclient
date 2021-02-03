package filesystem

import (
	"fmt"
	"time"
)

type FSEntryType string

const (
	FSFileEntry      FSEntryType = "file"
	FSDirectoryEntry FSEntryType = "directory"
)

// FSEntry ...
type FSEntry struct {
	ID         int64
	Type       FSEntryType
	Name       string
	Size       int64
	CreateTime time.Time
	ModifyTime time.Time
	CheckSum   string
	//
	Internal interface{} // IRODSDataObject or IRODSCollection
}

// ToString stringifies the object
func (entry *FSEntry) ToString() string {
	return fmt.Sprintf("<FSEntry %d %s %s %d %s %s>", entry.ID, entry.Type, entry.Name, entry.Size, entry.CreateTime, entry.ModifyTime)
}
