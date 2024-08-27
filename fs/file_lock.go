package fs

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/xerrors"
)

// this is a file lock managed by go-irodsclient. Not same as fs file lock

// FileLock is a lock for a file
type FileLock struct {
	path       string
	references int64
	mutex      sync.RWMutex // used to lock the file
}

// FileLocks manages file locks
type FileLocks struct {
	mutex sync.Mutex
	locks map[string]*FileLock
}

// NewFileLocks creates a new FileLocks
func NewFileLocks() *FileLocks {
	return &FileLocks{
		mutex: sync.Mutex{},
		locks: map[string]*FileLock{},
	}
}

// LockFilesForPrefix locks all files starting with the given prefix, does not create a new lock, but increases reference
func (mgr *FileLocks) LockFilesForPrefix(pathPrefix string) []string {
	fileLocks := []*FileLock{}

	mgr.mutex.Lock()

	prefix := fmt.Sprintf("%s/", pathPrefix)
	for _, lock := range mgr.locks {
		if strings.HasPrefix(lock.path, prefix) {
			fileLocks = append(fileLocks, lock)
			lock.references++
		}
	}

	mgr.mutex.Unlock()

	lockedFilePaths := []string{}
	for _, fileLock := range fileLocks {
		fileLock.mutex.Lock() // write lock
		lockedFilePaths = append(lockedFilePaths, fileLock.path)
	}

	return lockedFilePaths
}

// UnlockFiles unlocks multiple files
func (mgr *FileLocks) UnlockFiles(paths []string) error {
	fileLocks := []*FileLock{}

	mgr.mutex.Lock()

	for _, path := range paths {
		if lock, ok := mgr.locks[path]; ok {
			// fileLock already exists
			fileLocks = append(fileLocks, lock)

			if lock.references <= 0 {
				mgr.mutex.Unlock()
				return xerrors.Errorf("file lock for path %q has invalid references %d", path, lock.references)
			}

			lock.references--

			if lock.references == 0 {
				delete(mgr.locks, path)
			}
		} else {
			mgr.mutex.Unlock()
			return xerrors.Errorf("file lock for path %q does not exist", path)
		}
	}

	mgr.mutex.Unlock()

	// unlock in reverse order
	for i := len(fileLocks) - 1; i >= 0; i-- {
		fileLock := fileLocks[i]
		fileLock.mutex.Unlock() // unlock write lock
	}

	return nil
}

// Lock locks a file
func (mgr *FileLocks) Lock(path string) {
	var fileLock *FileLock

	mgr.mutex.Lock()

	if lock, ok := mgr.locks[path]; ok {
		// fileLock already exists
		fileLock = lock
		fileLock.references++
	} else {
		// create a new
		fileLock = &FileLock{
			path:       path,
			references: 1,
			mutex:      sync.RWMutex{},
		}
		mgr.locks[path] = fileLock
	}

	mgr.mutex.Unlock()

	fileLock.mutex.Lock() // write lock
}

// RLock locks a file with read mode
func (mgr *FileLocks) RLock(path string) {
	var fileLock *FileLock

	mgr.mutex.Lock()

	if lock, ok := mgr.locks[path]; ok {
		// fileLock already exists
		fileLock = lock
		fileLock.references++
	} else {
		// create a new
		fileLock = &FileLock{
			path:       path,
			references: 1,
			mutex:      sync.RWMutex{},
		}
		mgr.locks[path] = fileLock
	}

	mgr.mutex.Unlock()

	fileLock.mutex.RLock() // read lock
}

// Unlock unlocks a file
func (mgr *FileLocks) Unlock(path string) error {
	var fileLock *FileLock

	mgr.mutex.Lock()

	if lock, ok := mgr.locks[path]; ok {
		// fileLock already exists
		fileLock = lock
	} else {
		mgr.mutex.Unlock()
		return xerrors.Errorf("file lock for path %q does not exist", path)
	}

	if fileLock.references <= 0 {
		mgr.mutex.Unlock()
		return xerrors.Errorf("file lock for path %q has invalid references %d", path, fileLock.references)
	}

	fileLock.references--

	if fileLock.references == 0 {
		delete(mgr.locks, path)
	}

	mgr.mutex.Unlock()

	fileLock.mutex.Unlock()
	return nil
}

// RUnlock unlocks a file with read mode
func (mgr *FileLocks) RUnlock(path string) error {
	var fileLock *FileLock

	mgr.mutex.Lock()

	if lock, ok := mgr.locks[path]; ok {
		// fileLock already exists
		fileLock = lock
	} else {
		mgr.mutex.Unlock()
		return xerrors.Errorf("file lock for path %q does not exist", path)
	}

	if fileLock.references <= 0 {
		mgr.mutex.Unlock()
		return xerrors.Errorf("file lock for path %q has invalid references %d", path, fileLock.references)
	}

	fileLock.references--

	if fileLock.references == 0 {
		delete(mgr.locks, path)
	}

	mgr.mutex.Unlock()

	fileLock.mutex.RUnlock()
	return nil
}
