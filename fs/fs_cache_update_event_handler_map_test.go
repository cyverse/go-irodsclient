package fs

import (
	"testing"
	"time"
)

func TestSendEventAllowsReentrantHandlerMutation(t *testing.T) {
	handlerMap := NewFilesystemCacheEventHandlerMap()

	var handlerID string
	handlerID = handlerMap.AddEventHandler(func(path string, eventType FilesystemCacheEventType) {
		if path != "/file" || eventType != FilesystemCacheFileUpdateEvent {
			t.Errorf("unexpected event: path=%q type=%q", path, eventType)
		}

		handlerMap.RemoveEventHandler(handlerID)
		handlerMap.AddEventHandler(func(string, FilesystemCacheEventType) {})
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		handlerMap.SendFileUpdateEvent("/file")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SendEvent deadlocked when a handler mutated the handler map")
	}

	if handlerMap.GetEventHandler(handlerID) != nil {
		t.Fatal("reentrant callback did not remove itself")
	}
}

func TestSendEventAllowsReentrantRelease(t *testing.T) {
	handlerMap := NewFilesystemCacheEventHandlerMap()
	handlerMap.AddEventHandler(func(string, FilesystemCacheEventType) {
		handlerMap.Release()
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		handlerMap.SendFileUpdateEvent("/file")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SendEvent deadlocked when a handler released the handler map")
	}

	if len(handlerMap.GetEventHandlerIDs()) != 0 {
		t.Fatal("reentrant callback did not release the handler map")
	}
}
