package fs

import (
	"sync"
)

// FilesystemCacheUpdateEventType defines cache
type FilesystemCacheUpdateEventType string

const (
	// FilesystemCacheFileCreateEvent is an event type for file creation
	FilesystemCacheFileCreateEvent FilesystemCacheUpdateEventType = "file create"
	// FilesystemCacheFileRemoveEvent is an event type for file removal
	FilesystemCacheFileRemoveEvent FilesystemCacheUpdateEventType = "file remove"
	// FilesystemCacheFileUpdateEvent is an event type for file update
	FilesystemCacheFileUpdateEvent FilesystemCacheUpdateEventType = "file update"
	// FilesystemCacheDirCreateEvent is an event type for dir creation
	FilesystemCacheDirCreateEvent FilesystemCacheUpdateEventType = "dir create"
	// FilesystemCacheDirRemoveEvent is an event type for dir removal
	FilesystemCacheDirRemoveEvent FilesystemCacheUpdateEventType = "dir remove"
)

// these are used to sync caches between different fs instances
type FilesystemCacheUpdatedFunc func(path string, eventType FilesystemCacheUpdateEventType)

var (
	filesystemCacheUpdateEventHandlersMutex sync.Mutex
	filesystemCacheUpdateEventHandlers      map[string]FilesystemCacheUpdatedFunc
)

func init() {
	filesystemCacheUpdateEventHandlers = make(map[string]FilesystemCacheUpdatedFunc)
}

// FileSystemCachePropagation manages filesystem cache propagation
type FileSystemCachePropagation struct {
	filesystem *FileSystem
}

// NewFileSystemCachePropagation creates a new FileSystemCachePropagation
func NewFileSystemCachePropagation(fs *FileSystem) *FileSystemCachePropagation {
	cachePropagation := &FileSystemCachePropagation{
		filesystem: fs,
	}

	filesystemCacheUpdateEventHandlersMutex.Lock()
	defer filesystemCacheUpdateEventHandlersMutex.Unlock()

	filesystemCacheUpdateEventHandlers[fs.GetID()] = func(path string, eventType FilesystemCacheUpdateEventType) {
		go cachePropagation.handle(path, eventType)
	}

	return cachePropagation
}

func (propagation *FileSystemCachePropagation) Release() {
	filesystemCacheUpdateEventHandlersMutex.Lock()
	defer filesystemCacheUpdateEventHandlersMutex.Unlock()

	delete(filesystemCacheUpdateEventHandlers, propagation.filesystem.GetID())
}

func (propagation *FileSystemCachePropagation) handle(path string, eventType FilesystemCacheUpdateEventType) {
	switch eventType {
	case FilesystemCacheFileCreateEvent:
		propagation.filesystem.invalidateCacheForFileCreate(path)
	case FilesystemCacheFileRemoveEvent:
		propagation.filesystem.invalidateCacheForFileRemove(path)
	case FilesystemCacheFileUpdateEvent:
		propagation.filesystem.invalidateCacheForFileUpdate(path)
	case FilesystemCacheDirCreateEvent:
		propagation.filesystem.invalidateCacheForDirCreate(path)
	case FilesystemCacheDirRemoveEvent:
		propagation.filesystem.invalidateCacheForDirRemove(path, true)
	default:
		// unhandled
	}
}

// Propagate propagates fs cache update event
func (propagation *FileSystemCachePropagation) Propagate(path string, eventType FilesystemCacheUpdateEventType) {
	filesystemCacheUpdateEventHandlersMutex.Lock()
	defer filesystemCacheUpdateEventHandlersMutex.Unlock()

	for fsID, handler := range filesystemCacheUpdateEventHandlers {
		if fsID != propagation.filesystem.GetID() {
			handler(path, eventType)
		}
	}
}

// PropagateDirCreate propagates fs cache update event for dir create
func (propagation *FileSystemCachePropagation) PropagateDirCreate(path string) {
	propagation.Propagate(path, FilesystemCacheDirCreateEvent)
}

// PropagateDirRemove propagates fs cache update event for dir remove
func (propagation *FileSystemCachePropagation) PropagateDirRemove(path string) {
	propagation.Propagate(path, FilesystemCacheDirRemoveEvent)
}

// PropagateFileCreate propagates fs cache update event for file create
func (propagation *FileSystemCachePropagation) PropagateFileCreate(path string) {
	propagation.Propagate(path, FilesystemCacheFileCreateEvent)
}

// PropagateFileRemove propagates fs cache update event for file remove
func (propagation *FileSystemCachePropagation) PropagateFileRemove(path string) {
	propagation.Propagate(path, FilesystemCacheFileRemoveEvent)
}

// PropagateFileUpdate propagates fs cache update event for file update
func (propagation *FileSystemCachePropagation) PropagateFileUpdate(path string) {
	propagation.Propagate(path, FilesystemCacheFileUpdateEvent)
}
