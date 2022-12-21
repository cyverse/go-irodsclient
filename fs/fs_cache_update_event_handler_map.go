package fs

import (
	"sync"

	"github.com/rs/xid"
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

type FilesystemCacheUpdateEventHandler func(path string, eventType FilesystemCacheUpdateEventType)

// FilesystemCacheUpdateEventHandlerMap manages FilesystemCacheUpdateEventHandler
type FilesystemCacheUpdateEventHandlerMap struct {
	mutex    sync.RWMutex
	handlers map[string]FilesystemCacheUpdateEventHandler // ID-handler mapping
}

// NewFilesystemCacheUpdateEventHandlerMap creates a new FilesystemCacheUpdateEventHandlerMap
func NewFilesystemCacheUpdateEventHandlerMap() *FilesystemCacheUpdateEventHandlerMap {
	return &FilesystemCacheUpdateEventHandlerMap{
		mutex:    sync.RWMutex{},
		handlers: map[string]FilesystemCacheUpdateEventHandler{},
	}
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) Release() {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers = map[string]FilesystemCacheUpdateEventHandler{}
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) AddEventHandler(handler FilesystemCacheUpdateEventHandler) string {
	handlerID := xid.New().String()

	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers[handlerID] = handler

	return handlerID
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) RemoveEventHandler(handlerID string) {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	delete(handlerMap.handlers, handlerID)
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) GetEventHandler(handlerID string) FilesystemCacheUpdateEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	return handlerMap.handlers[handlerID]
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) GetEventHandlers() []FilesystemCacheUpdateEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlers := []FilesystemCacheUpdateEventHandler{}
	for _, handler := range handlerMap.handlers {
		handlers = append(handlers, handler)
	}

	return handlers
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) GetEventHandlerIDs() []string {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlerIDs := []string{}
	for handlerID, _ := range handlerMap.handlers {
		handlerIDs = append(handlerIDs, handlerID)
	}

	return handlerIDs
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendEvent(path string, eventType FilesystemCacheUpdateEventType) {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	for _, handler := range handlerMap.handlers {
		handler(path, eventType)
	}
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendFileCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileCreateEvent)
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendFileRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileRemoveEvent)
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendFileUpdateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileUpdateEvent)
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendDirCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirCreateEvent)
}

func (handlerMap *FilesystemCacheUpdateEventHandlerMap) SendDirRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirRemoveEvent)
}
