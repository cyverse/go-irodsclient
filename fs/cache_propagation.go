package fs

import (
	"sync"
)

// used to sync caches between different fs instances

var (
	filesystemCacheUpdateEventHandlersMutex sync.RWMutex
	filesystemCacheUpdateEventHandlers      map[string]FilesystemCacheUpdateEventHandler
)

func init() {
	filesystemCacheUpdateEventHandlersMutex = sync.RWMutex{}
	filesystemCacheUpdateEventHandlers = make(map[string]FilesystemCacheUpdateEventHandler)
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
	filesystemCacheUpdateEventHandlersMutex.RLock()
	defer filesystemCacheUpdateEventHandlersMutex.RUnlock()

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
