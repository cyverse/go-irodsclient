package fs

import (
	"testing"
	"time"
)

func completesFileHandleMapOperation(t *testing.T, operation func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		operation()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("file handle map operation deadlocked")
	}
}

func TestAddCloseEventHandlerAllowsReentrantCallback(t *testing.T) {
	handleMap := NewFileHandleMap()

	completesFileHandleMapOperation(t, func() {
		handleMap.AddCloseEventHandler("/empty", func(path string, id string, empty bool) {
			if path != "/empty" || id != "" || !empty {
				t.Errorf("unexpected event: path=%q id=%q empty=%t", path, id, empty)
			}
			_ = handleMap.List()
		})
	})
}

func TestRemoveAllowsReentrantCloseEventHandler(t *testing.T) {
	handleMap := NewFileHandleMap()
	handle := &FileHandle{id: "handle-1", entry: &Entry{Path: "/file"}}
	handleMap.Add(handle)

	var handlerID string
	handlerID = handleMap.AddCloseEventHandler("/file", func(path string, id string, empty bool) {
		if path != "/file" || id != handle.id || !empty {
			t.Errorf("unexpected event: path=%q id=%q empty=%t", path, id, empty)
		}
		_ = handleMap.Get(id)
		handleMap.RemoveCloseEventHandler(handlerID)
	})

	completesFileHandleMapOperation(t, func() {
		handleMap.Remove(handle.id)
	})

	if handleMap.Get(handle.id) != nil {
		t.Fatal("removed handle is still registered")
	}
	if _, ok := handleMap.eventHandlerIDPath[handlerID]; ok {
		t.Fatal("reentrant callback did not remove its event handler")
	}
}
