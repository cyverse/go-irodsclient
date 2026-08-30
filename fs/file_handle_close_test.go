package fs

import (
	"errors"
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
