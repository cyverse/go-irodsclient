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

type EntryReplica struct {
}

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
	AccessTime        time.Time               `json:"access_time"`
	CheckSumAlgorithm types.ChecksumAlgorithm `json:"checksum_algorithm"`
	CheckSum          []byte                  `json:"checksum"`
	IRODSReplicas     []types.IRODSReplica    `json:"replicas,omitempty"`
}

func NewEntryFromCollection(collection *types.IRODSCollection) *Entry {
	return &Entry{
		ID:                collection.ID,
		Type:              DirectoryEntry,
		Name:              collection.Name,
		Path:              collection.Path,
		Owner:             collection.Owner,
		Size:              0,
		DataType:          "",
		CreateTime:        collection.CreateTime,
		ModifyTime:        collection.ModifyTime,
		AccessTime:        collection.ModifyTime, // default to modify time
		CheckSumAlgorithm: types.ChecksumAlgorithmUnknown,
		CheckSum:          nil,
		IRODSReplicas:     nil,
	}
}

func NewEntryFromDataObject(dataobject *types.IRODSDataObject) *Entry {
	checksum := dataobject.Replicas[0].Checksum

	checksumAlgorithm := types.ChecksumAlgorithmUnknown
	var checksumString []byte

	if checksum != nil && len(checksum.Checksum) > 0 {
		checksumAlgorithm = checksum.Algorithm
		checksumString = checksum.Checksum
	}

	replicas := []types.IRODSReplica{}
	for _, replica := range dataobject.Replicas {
		replicas = append(replicas, *replica)
	}

	return &Entry{
		ID:                dataobject.ID,
		Type:              FileEntry,
		Name:              dataobject.Name,
		Path:              dataobject.Path,
		Owner:             dataobject.Replicas[0].Owner,
		Size:              dataobject.Size,
		DataType:          dataobject.DataType,
		CreateTime:        dataobject.Replicas[0].CreateTime,
		ModifyTime:        dataobject.Replicas[0].ModifyTime,
		AccessTime:        dataobject.Replicas[0].AccessTime,
		CheckSumAlgorithm: checksumAlgorithm,
		CheckSum:          checksumString,
		IRODSReplicas:     replicas,
	}
}

// ToString stringifies the object
func (entry *Entry) ToString() string {
	return fmt.Sprintf("<Entry %d %s %s %s %d %s %s %s %s>", entry.ID, entry.Type, entry.Path, entry.Owner, entry.Size, entry.DataType, entry.CreateTime, entry.ModifyTime, entry.AccessTime)
}

// IsDir returns if the entry is for directory
func (entry *Entry) IsDir() bool {
	return entry.Type == DirectoryEntry
}

// ToCollection returns collection
func (entry *Entry) ToCollection() *types.IRODSCollection {
	return &types.IRODSCollection{
		ID:         entry.ID,
		Path:       entry.Path,
		Name:       entry.Name,
		Owner:      entry.Owner,
		CreateTime: entry.CreateTime,
		ModifyTime: entry.ModifyTime,
	}
}

// ToDataObject returns data object
func (entry *Entry) ToDataObject() *types.IRODSDataObject {
	replicas := []*types.IRODSReplica{}
	for _, replica := range entry.IRODSReplicas {
		replicas = append(replicas, &replica)
	}

	return &types.IRODSDataObject{
		ID:       entry.ID,
		Path:     entry.Path,
		Name:     entry.Name,
		Size:     entry.Size,
		DataType: entry.DataType,
		Replicas: replicas,
	}
}
