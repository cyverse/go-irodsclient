package fs

import (
	"bytes"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/types"
)

func TestEntryCloneDoesNotShareMutableFields(t *testing.T) {
	entry := &Entry{
		Path:     "/file",
		Size:     10,
		CheckSum: []byte{1, 2, 3},
		IRODSReplicas: []types.IRODSReplica{{
			Number: 1,
			Checksum: &types.IRODSChecksum{
				Checksum: []byte{4, 5, 6},
			},
		}},
	}

	cloned := entry.clone()
	cloned.Size = 20
	cloned.CheckSum[0] = 9
	cloned.IRODSReplicas[0].Number = 2
	cloned.IRODSReplicas[0].Checksum.Checksum[0] = 8

	if entry.Size != 10 || !bytes.Equal(entry.CheckSum, []byte{1, 2, 3}) {
		t.Fatal("clone mutation changed the original entry")
	}
	if entry.IRODSReplicas[0].Number != 1 || !bytes.Equal(entry.IRODSReplicas[0].Checksum.Checksum, []byte{4, 5, 6}) {
		t.Fatal("clone mutation changed the original replica")
	}
}

func TestEntryCacheDoesNotExposeStoredEntry(t *testing.T) {
	config := NewDefaultCacheConfig()
	cache := NewFileSystemCache(&config, "entry-clone-test")
	defer cache.Close()

	entry := &Entry{Path: "/file", Size: 10}
	cache.AddEntryCache(entry)
	entry.Size = 20

	first := cache.GetEntryCache(entry.Path)
	if first == nil || first.Size != 10 {
		t.Fatalf("cached entry size = %v, want 10", first)
	}
	first.Size = 30

	second := cache.GetEntryCache(entry.Path)
	if second == nil || second.Size != 10 {
		t.Fatalf("cache was changed through returned entry: %#v", second)
	}
}

func TestFileHandleGetEntryReturnsSnapshot(t *testing.T) {
	handle := &FileHandle{entry: &Entry{Path: "/file", Size: 10}}

	entry := handle.GetEntry()
	entry.Size = 20

	if handle.GetEntry().Size != 10 {
		t.Fatal("GetEntry exposed the file handle's mutable entry")
	}
}
