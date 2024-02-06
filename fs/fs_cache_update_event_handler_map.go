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
	// FilesystemCacheDirExtractEvent is an event type for dir extract
	FilesystemCacheDirExtractEvent FilesystemCacheEventType = "dir extract"
)

// FilesystemCacheEventHandler is a cache event handler type
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

// Release releases resources
func (handlerMap *FilesystemCacheEventHandlerMap) Release() {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers = map[string]FilesystemCacheEventHandler{}
}

// AddEventHandler adds cache eventh handler
func (handlerMap *FilesystemCacheEventHandlerMap) AddEventHandler(handler FilesystemCacheEventHandler) string {
	handlerID := xid.New().String()

	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	handlerMap.handlers[handlerID] = handler

	return handlerID
}

// RemoveEventHandler removes cache eventh handler
func (handlerMap *FilesystemCacheEventHandlerMap) RemoveEventHandler(handlerID string) {
	handlerMap.mutex.Lock()
	defer handlerMap.mutex.Unlock()

	delete(handlerMap.handlers, handlerID)
}

// GetEventHandler returns event handler for the id
func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandler(handlerID string) FilesystemCacheEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	return handlerMap.handlers[handlerID]
}

// GetEventHandlers returns all event handlers
func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandlers() []FilesystemCacheEventHandler {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlers := []FilesystemCacheEventHandler{}
	for _, handler := range handlerMap.handlers {
		handlers = append(handlers, handler)
	}

	return handlers
}

// GetEventHandlerIDs returns all event handler ids
func (handlerMap *FilesystemCacheEventHandlerMap) GetEventHandlerIDs() []string {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	handlerIDs := []string{}
	for handlerID := range handlerMap.handlers {
		handlerIDs = append(handlerIDs, handlerID)
	}

	return handlerIDs
}

// SendEvent sends event
func (handlerMap *FilesystemCacheEventHandlerMap) SendEvent(path string, eventType FilesystemCacheEventType) {
	handlerMap.mutex.RLock()
	defer handlerMap.mutex.RUnlock()

	for _, handler := range handlerMap.handlers {
		handler(path, eventType)
	}
}

// SendFileCreateEvent sends file create event
func (handlerMap *FilesystemCacheEventHandlerMap) SendFileCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileCreateEvent)
}

// SendFileRemoveEvent sends file remove event
func (handlerMap *FilesystemCacheEventHandlerMap) SendFileRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileRemoveEvent)
}

// SendFileUpdateEvent sends file update event
func (handlerMap *FilesystemCacheEventHandlerMap) SendFileUpdateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheFileUpdateEvent)
}

// SendDirCreateEvent sends dir create event
func (handlerMap *FilesystemCacheEventHandlerMap) SendDirCreateEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirCreateEvent)
}

// SendDirRemoveEvent sends dir remove event
func (handlerMap *FilesystemCacheEventHandlerMap) SendDirRemoveEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirRemoveEvent)
}

// SendDirExtractEvent sends dir extract event
func (handlerMap *FilesystemCacheEventHandlerMap) SendDirExtractEvent(path string) {
	handlerMap.SendEvent(path, FilesystemCacheDirExtractEvent)
}
