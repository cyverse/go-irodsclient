package fs

import (
	"fmt"
	"strings"
	"sync"
)

// FileLockHandleMap manages File Lock Handles opened
type FileLockHandleMap struct {
	mutex           sync.RWMutex
	fileLockHandles map[string]*FileLockHandle // ID-handle mapping
	filePathID      map[string][]string        // path-IDs mappings
}

// NewFileLockHandleMap creates a new FileLockHandleMap
func NewFileLockHandleMap() *FileLockHandleMap {
	return &FileLockHandleMap{
		mutex:           sync.RWMutex{},
		fileLockHandles: map[string]*FileLockHandle{},
		filePathID:      map[string][]string{},
	}
}

// Add registers a file lock handle
func (fileLockHandleMap *FileLockHandleMap) Add(handle *FileLockHandle) {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	fileLockHandleMap.fileLockHandles[handle.id] = handle
	if ids, ok := fileLockHandleMap.filePathID[handle.entry.Path]; ok {
		fileLockHandleMap.filePathID[handle.entry.Path] = append(ids, handle.id)
	} else {
		fileLockHandleMap.filePathID[handle.entry.Path] = []string{handle.id}
	}
}

// Remove deletes a file lock handle registered using ID
func (fileLockHandleMap *FileLockHandleMap) Remove(id string) {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	handle := fileLockHandleMap.fileLockHandles[id]
	if handle != nil {
		delete(fileLockHandleMap.fileLockHandles, id)

		if ids, ok := fileLockHandleMap.filePathID[handle.entry.Path]; ok {
			newIDs := []string{}
			for _, handleID := range ids {
				if handleID != id {
					newIDs = append(newIDs, handleID)
				}
			}

			if len(newIDs) > 0 {
				fileLockHandleMap.filePathID[handle.entry.Path] = newIDs
			} else {
				delete(fileLockHandleMap.filePathID, handle.entry.Path)
			}
		}
	}
}

// PopAll pops all file lock handles registered (clear) and returns
func (fileLockHandleMap *FileLockHandleMap) PopAll() []*FileLockHandle {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	handles := []*FileLockHandle{}
	for _, handle := range fileLockHandleMap.fileLockHandles {
		handles = append(handles, handle)
	}

	// clear
	fileLockHandleMap.fileLockHandles = map[string]*FileLockHandle{}
	fileLockHandleMap.filePathID = map[string][]string{}

	return handles
}

// Clear clears all file lock handles registered
func (fileLockHandleMap *FileLockHandleMap) Clear() {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	fileLockHandleMap.fileLockHandles = map[string]*FileLockHandle{}
	fileLockHandleMap.filePathID = map[string][]string{}
}

// List lists all file lock handles registered
func (fileLockHandleMap *FileLockHandleMap) List() []*FileLockHandle {
	fileLockHandleMap.mutex.RLock()
	defer fileLockHandleMap.mutex.RUnlock()

	handles := []*FileLockHandle{}
	for _, handle := range fileLockHandleMap.fileLockHandles {
		handles = append(handles, handle)
	}

	return handles
}

// Get returns a file lock handle registered using ID
func (fileLockHandleMap *FileLockHandleMap) Get(id string) *FileLockHandle {
	fileLockHandleMap.mutex.RLock()
	defer fileLockHandleMap.mutex.RUnlock()

	return fileLockHandleMap.fileLockHandles[id]
}

// Pop pops a file lock handle registered using ID and returns the handle
func (fileLockHandleMap *FileLockHandleMap) Pop(id string) *FileLockHandle {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	handle := fileLockHandleMap.fileLockHandles[id]
	if handle != nil {
		delete(fileLockHandleMap.fileLockHandles, id)

		if ids, ok := fileLockHandleMap.filePathID[handle.entry.Path]; ok {
			newIDs := []string{}
			for _, handleID := range ids {
				if handleID != id {
					newIDs = append(newIDs, handleID)
				}
			}

			if len(newIDs) > 0 {
				fileLockHandleMap.filePathID[handle.entry.Path] = newIDs
			} else {
				delete(fileLockHandleMap.filePathID, handle.entry.Path)
			}
		}
	}

	return handle
}

// ListByPath returns file lock handles registered using path
func (fileLockHandleMap *FileLockHandleMap) ListByPath(path string) []*FileLockHandle {
	fileLockHandleMap.mutex.RLock()
	defer fileLockHandleMap.mutex.RUnlock()

	handles := []*FileLockHandle{}
	if ids, ok := fileLockHandleMap.filePathID[path]; ok {
		for _, handleID := range ids {
			if handle, ok2 := fileLockHandleMap.fileLockHandles[handleID]; ok2 {
				handles = append(handles, handle)
			}
		}
	}
	return handles
}

// ListPathsUnderDir returns paths of file lock handles under given parent path
func (fileLockHandleMap *FileLockHandleMap) ListPathsInDir(parentPath string) []string {
	fileLockHandleMap.mutex.RLock()
	defer fileLockHandleMap.mutex.RUnlock()

	prefix := parentPath
	if len(prefix) > 1 && !strings.HasSuffix(prefix, "/") {
		prefix = fmt.Sprintf("%s/", prefix)
	}

	paths := []string{}
	// loop over all file handles opened
	for path := range fileLockHandleMap.filePathID {
		// check if it's sub dirs or files in the dir
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}

	return paths
}

// PopByPath pops file lock handles registered using path and returns the handles
func (fileLockHandleMap *FileLockHandleMap) PopByPath(path string) []*FileLockHandle {
	fileLockHandleMap.mutex.Lock()
	defer fileLockHandleMap.mutex.Unlock()

	handles := []*FileLockHandle{}
	if ids, ok := fileLockHandleMap.filePathID[path]; ok {
		for _, handleID := range ids {
			if handle, ok2 := fileLockHandleMap.fileLockHandles[handleID]; ok2 {
				handles = append(handles, handle)
				delete(fileLockHandleMap.fileLockHandles, handleID)
			}
		}

		delete(fileLockHandleMap.filePathID, path)
	}

	return handles
}
