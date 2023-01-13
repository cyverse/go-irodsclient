package fs

import (
	"sync"

	"github.com/rs/xid"
)

// FilesystemCacheEventType defines cache
type FilesystemCacheEventType string

const (
	// FilesystemCacheFileCreateEvent is an event type for file creation
	FilesystemCacheFileCreateEvent FilesystemCacheEventType = "file create"
	// FilesystemCacheFileRemoveEvent is an event type for file removal
	FilesystemCacheFileRemoveEvent FilesystemCacheEventType = "file remove"
	// FilesystemCacheFileUpdateEvent is an event type for file update
	FilesystemCacheFileUpdateEvent FilesystemCacheEventType = "file update"
	// FilesystemCacheDirCreateEvent is an event type for dir creation
	FilesystemCacheDirCreateEvent FilesystemCacheEventType = "dir create"
	// FilesystemCacheDirRemoveEvent is an event type for dir removal
	FilesystemCacheDirRemoveEvent FilesystemCacheEventType = "dir remove"
)

type FilesystemCacheEventHandler func(path string, eventType FilesystemCacheEventType)

// FilesystemCacheEventHandlerMap manages FilesystemCacheEventHandler
type FilesystemCacheEventHandlerMap struct {
	mutex    sync.RWMutex
	handlers map[string]FilesystemCacheEventHandler // ID-handler mapping
}

// NewFilesystemCacheEventHandlerMap creates a new FilesystemCacheEventHandlerMap
func NewFilesystemCacheEventHandlerMap() *FilesystemCacheEventHandlerMap {
	return &FilesystemCacheEventHandlerMap{
		mutex:    sync.RWMutex{},
		handlers: map[string]FilesystemCacheEventHandler{},
	}
}

func (handlerMap *FilesystemCacheEventHandlerMap) Release() {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers = map[string]FilesystemCacheEventHandler{}
}

func (handlerMap *FilesystemCacheEventHandlerMap) AddEventHandler(handler FilesystemCacheEventHandler) string {
	handlerID := xid.New().String()

	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers[handlerID] = handler

	return handlerID
}

func (handlerMap *FilesystemCacheEventHandlerMap) RemoveEventHandler(handlerID string) {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	delete(handlerMap.handlers, handlerID)
}

func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandler(handlerID string) FilesystemCacheEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	return handlerMap.handlers[handlerID]
}

func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandlers() []FilesystemCacheEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlers := []FilesystemCacheEventHandler{}
	for _, handler := range handlerMap.handlers {
		handlers = append(handlers, handler)
	}

	return handlers
}

func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandlerIDs() []string {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlerIDs := []string{}
	for handlerID, _ := range handlerMap.handlers {
		handlerIDs = append(handlerIDs, handlerID)
	}

	return handlerIDs
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendEvent(path string, eventType FilesystemCacheEventType) {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	for _, handler := range handlerMap.handlers {
		handler(path, eventType)
	}
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendFileCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileCreateEvent)
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendFileRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileRemoveEvent)
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendFileUpdateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileUpdateEvent)
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendDirCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirCreateEvent)
}

func (handlerMap *FilesystemCacheEventHandlerMap) SendDirRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirRemoveEvent)
}
