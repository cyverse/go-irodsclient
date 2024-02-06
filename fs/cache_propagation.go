package fs

import (
	"sync"
)

// used to sync caches between different fs instances

var (
	filesystemCacheEventHandlersMutex sync.RWMutex
	filesystemCacheEventHandlers      map[string]FilesystemCacheEventHandler
)

func init() {
	filesystemCacheEventHandlersMutex = sync.RWMutex{}
	filesystemCacheEventHandlers = make(map[string]FilesystemCacheEventHandler)
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

	filesystemCacheEventHandlersMutex.Lock()
	defer filesystemCacheEventHandlersMutex.Unlock()

	filesystemCacheEventHandlers[fs.GetID()] = func(path string, eventType FilesystemCacheEventType) {
		go cachePropagation.handle(path, eventType)
	}

	return cachePropagation
}

// Release releases resources
func (propagation *FileSystemCachePropagation) Release() {
	filesystemCacheEventHandlersMutex.Lock()
	defer filesystemCacheEventHandlersMutex.Unlock()

	delete(filesystemCacheEventHandlers, propagation.filesystem.GetID())
}

func (propagation *FileSystemCachePropagation) handle(path string, eventType FilesystemCacheEventType) {
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
	case FilesystemCacheDirExtractEvent:
		propagation.filesystem.invalidateCacheForDirExtract(path)
	default:
		// unhandled
	}
}

// Propagate propagates fs cache update event
func (propagation *FileSystemCachePropagation) Propagate(path string, eventType FilesystemCacheEventType) {
	filesystemCacheEventHandlersMutex.RLock()
	defer filesystemCacheEventHandlersMutex.RUnlock()

	for fsID, handler := range filesystemCacheEventHandlers {
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

// PropagateDirExtract propagates fs cache update event for dir extract
func (propagation *FileSystemCachePropagation) PropagateDirExtract(path string) {
	propagation.Propagate(path, FilesystemCacheDirExtractEvent)
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
