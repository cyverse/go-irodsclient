package fs

import (
	"reflect"
	"sync"
	"testing"
)

func TestLockFilesForPrefixUsesStablePathOrder(t *testing.T) {
	mgr := NewFileLocks()
	for _, path := range []string{"/dir/c", "/dir/a", "/dir/b"} {
		mgr.locks[path] = &FileLock{path: path, mutex: sync.RWMutex{}}
	}

	lockedPaths := mgr.LockFilesForPrefix("/dir")
	want := []string{"/dir/a", "/dir/b", "/dir/c"}
	if !reflect.DeepEqual(lockedPaths, want) {
		t.Fatalf("locked paths = %v, want stable order %v", lockedPaths, want)
	}

	if err := mgr.UnlockFiles(lockedPaths); err != nil {
		t.Fatalf("failed to unlock paths: %v", err)
	}
}

func TestFileLocksRejectsWriteUnlockForReadLock(t *testing.T) {
	mgr := NewFileLocks()
	mgr.RLock("/file")

	if err := mgr.Unlock("/file"); err == nil {
		t.Fatal("Unlock accepted a read lock")
	}
	if err := mgr.RUnlock("/file"); err != nil {
		t.Fatalf("failed to release read lock after rejected Unlock: %v", err)
	}
}

func TestFileLocksRejectsReadUnlockForWriteLock(t *testing.T) {
	mgr := NewFileLocks()
	mgr.Lock("/file")

	if err := mgr.RUnlock("/file"); err == nil {
		t.Fatal("RUnlock accepted a write lock")
	}
	if err := mgr.Unlock("/file"); err != nil {
		t.Fatalf("failed to release write lock after rejected RUnlock: %v", err)
	}
}
