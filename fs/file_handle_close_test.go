package fs

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestExecuteFileHandleCloseOperationsClosesAfterUnlockFailure(t *testing.T) {
	unlockErr := errors.New("unlock failed")
	closeErr := errors.New("close failed")
	closeCalled := false

	err := executeFileHandleCloseOperations(
		func() error { return unlockErr },
		func() error {
			closeCalled = true
			return closeErr
		},
	)

	if !closeCalled {
		t.Fatal("close operation was skipped after unlock failure")
	}
	if !errors.Is(err, unlockErr) {
		t.Fatalf("combined error does not contain unlock error: %v", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("combined error does not contain close error: %v", err)
	}
}

func TestExecuteFileHandleCloseOperationsWithoutLock(t *testing.T) {
	closeCalled := false
	err := executeFileHandleCloseOperations(nil, func() error {
		closeCalled = true
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !closeCalled {
		t.Fatal("close operation was not called")
	}
}

func TestFileHandleCloseOnceIsIdempotent(t *testing.T) {
	handle := &FileHandle{}
	wantErr := errors.New("close failed")
	var calls atomic.Int32

	closeHandle := func() error {
		handle.mutex.Lock()
		defer handle.mutex.Unlock()
		return handle.closeOnce(func() error {
			calls.Add(1)
			return wantErr
		})
	}

	const callers = 100
	var wait sync.WaitGroup
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- closeHandle()
		}()
	}
	wait.Wait()
	close(results)

	if calls.Load() != 1 {
		t.Fatalf("close operation ran %d times, want 1", calls.Load())
	}
	for err := range results {
		if !errors.Is(err, wantErr) {
			t.Fatalf("duplicate close returned %v, want %v", err, wantErr)
		}
	}
}
