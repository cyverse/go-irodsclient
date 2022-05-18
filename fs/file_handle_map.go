package fs

import (
	"fmt"
	"strings"
	"sync"

	"github.com/rs/xid"
)

// FileHandleMapEventHandler is a event handler for FileHandleMap
type FileHandleMapEventHandler func(path string, id string, empty bool)

type FileHandleMapEventHandlerWrap struct {
	id      string
	path    string
	handler FileHandleMapEventHandler
}

// FileHandleMap manages File Handles opened
type FileHandleMap struct {
	mutex              sync.Mutex
	fileHandles        map[string]*FileHandle                      // ID-handle mapping
	filePathID         map[string][]string                         // path-IDs mappings
	closeEventHandlers map[string][]*FileHandleMapEventHandlerWrap // path-eventhandler mapping
	eventHandlerIDPath map[string]string                           // eventhandlerID-path mappings
}

// NewFileHandleMap creates a new FileHandleMap
func NewFileHandleMap() *FileHandleMap {
	return &FileHandleMap{
		mutex:              sync.Mutex{},
		fileHandles:        map[string]*FileHandle{},
		filePathID:         map[string][]string{},
		closeEventHandlers: map[string][]*FileHandleMapEventHandlerWrap{},
		eventHandlerIDPath: map[string]string{},
	}
}

// AddCloseEventHandler registers an event handler for file close
func (fileHandleMap *FileHandleMap) AddCloseEventHandler(path string, handler FileHandleMapEventHandler) string {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handlerID := xid.New().String()

	handlerWrap := FileHandleMapEventHandlerWrap{
		id:      handlerID,
		path:    path,
		handler: handler,
	}

	if handlers, ok := fileHandleMap.closeEventHandlers[path]; ok {
		fileHandleMap.closeEventHandlers[path] = append(handlers, &handlerWrap)
	} else {
		fileHandleMap.closeEventHandlers[path] = []*FileHandleMapEventHandlerWrap{&handlerWrap}
	}

	fileHandleMap.eventHandlerIDPath[handlerID] = path

	// if there's no files?
	// raise event with empty id string
	if _, ok := fileHandleMap.filePathID[path]; !ok {
		handler(path, "", true)
	}

	return handlerID
}

// RemoveCloseEventHandler deregisters an event handler for file close
func (fileHandleMap *FileHandleMap) RemoveCloseEventHandler(handlerID string) {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	if path, ok := fileHandleMap.eventHandlerIDPath[handlerID]; ok {
		delete(fileHandleMap.eventHandlerIDPath, handlerID)

		newEventHandlers := []*FileHandleMapEventHandlerWrap{}
		if handlers, ok2 := fileHandleMap.closeEventHandlers[path]; ok2 {
			for _, handler := range handlers {
				if handler.id != handlerID {
					newEventHandlers = append(newEventHandlers, handler)
				}
			}

			if len(newEventHandlers) > 0 {
				fileHandleMap.closeEventHandlers[path] = newEventHandlers
			} else {
				delete(fileHandleMap.closeEventHandlers, path)
			}
		}
	}
}

// Add registers a file handle
func (fileHandleMap *FileHandleMap) Add(handle *FileHandle) {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	fileHandleMap.fileHandles[handle.id] = handle
	if ids, ok := fileHandleMap.filePathID[handle.entry.Path]; ok {
		fileHandleMap.filePathID[handle.entry.Path] = append(ids, handle.id)
	} else {
		fileHandleMap.filePathID[handle.entry.Path] = []string{handle.id}
	}
}

// Remove deletes a file handle registered using ID
func (fileHandleMap *FileHandleMap) Remove(id string) {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handle := fileHandleMap.fileHandles[id]
	if handle != nil {
		delete(fileHandleMap.fileHandles, id)

		emptyHandles := true
		if ids, ok := fileHandleMap.filePathID[handle.entry.Path]; ok {
			emptyHandles = false
			newIDs := []string{}
			for _, handleID := range ids {
				if handleID != id {
					newIDs = append(newIDs, handleID)
				}
			}

			if len(newIDs) > 0 {
				fileHandleMap.filePathID[handle.entry.Path] = newIDs
			} else {
				delete(fileHandleMap.filePathID, handle.entry.Path)
				emptyHandles = true
			}
		}

		if handlers, ok := fileHandleMap.closeEventHandlers[handle.entry.Path]; ok {
			for _, handler := range handlers {
				handler.handler(handle.entry.Path, id, emptyHandles)
			}
		}
	}
}

// PopAll pops all file handles registered (clear) and returns
func (fileHandleMap *FileHandleMap) PopAll() []*FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handles := []*FileHandle{}
	for _, handle := range fileHandleMap.fileHandles {
		handles = append(handles, handle)
	}

	// clear
	fileHandleMap.fileHandles = map[string]*FileHandle{}
	fileHandleMap.filePathID = map[string][]string{}

	return handles
}

// Clear clears all file handles registered
func (fileHandleMap *FileHandleMap) Clear() {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	fileHandleMap.fileHandles = map[string]*FileHandle{}
	fileHandleMap.filePathID = map[string][]string{}
}

// List lists all file handles registered
func (fileHandleMap *FileHandleMap) List() []*FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handles := []*FileHandle{}
	for _, handle := range fileHandleMap.fileHandles {
		handles = append(handles, handle)
	}

	return handles
}

// Get returns a file handle registered using ID
func (fileHandleMap *FileHandleMap) Get(id string) *FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	return fileHandleMap.fileHandles[id]
}

// Pop pops a file handle registered using ID and returns the handle
func (fileHandleMap *FileHandleMap) Pop(id string) *FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handle := fileHandleMap.fileHandles[id]
	if handle != nil {
		delete(fileHandleMap.fileHandles, id)

		if ids, ok := fileHandleMap.filePathID[handle.entry.Path]; ok {
			newIDs := []string{}
			for _, handleID := range ids {
				if handleID != id {
					newIDs = append(newIDs, handleID)
				}
			}

			if len(newIDs) > 0 {
				fileHandleMap.filePathID[handle.entry.Path] = newIDs
			} else {
				delete(fileHandleMap.filePathID, handle.entry.Path)
			}
		}
	}

	return handle
}

// ListByPath returns file handles registered using path
func (fileHandleMap *FileHandleMap) ListByPath(path string) []*FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handles := []*FileHandle{}
	if ids, ok := fileHandleMap.filePathID[path]; ok {
		for _, handleID := range ids {
			if handle, ok2 := fileHandleMap.fileHandles[handleID]; ok2 {
				handles = append(handles, handle)
			}
		}
	}
	return handles
}

// ListPathsUnderDir returns paths of file handles under given parent path
func (fileHandleMap *FileHandleMap) ListPathsInDir(parentPath string) []string {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	prefix := parentPath
	if len(prefix) > 1 && !strings.HasSuffix(prefix, "/") {
		prefix = fmt.Sprintf("%s/", prefix)
	}

	paths := []string{}
	// loop over all file handles opened
	for path := range fileHandleMap.filePathID {
		// check if it's sub dirs or files in the dir
		if strings.HasPrefix(path, prefix) {
			paths = append(paths, path)
		}
	}

	return paths
}

// PopByPath pops file handles registered using path and returns the handles
func (fileHandleMap *FileHandleMap) PopByPath(path string) []*FileHandle {
	fileHandleMap.mutex.Lock()
	defer fileHandleMap.mutex.Unlock()

	handles := []*FileHandle{}
	if ids, ok := fileHandleMap.filePathID[path]; ok {
		for _, handleID := range ids {
			if handle, ok2 := fileHandleMap.fileHandles[handleID]; ok2 {
				handles = append(handles, handle)
				delete(fileHandleMap.fileHandles, handleID)
			}
		}

		delete(fileHandleMap.filePathID, path)
	}

	return handles
}
