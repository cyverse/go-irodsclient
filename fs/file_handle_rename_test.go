package fs

import "testing"

func TestCloseFileHandleOnRenameFailure(t *testing.T) {
	closeCalls := 0
	closeFileHandleOnRenameFailure(false, func() {
		closeCalls++
	})

	if closeCalls != 1 {
		t.Fatalf("temporary rename handle was closed %d times, want 1", closeCalls)
	}
}

func TestKeepFileHandleAfterRenameOwnershipTransfer(t *testing.T) {
	closeCalls := 0
	closeFileHandleOnRenameFailure(true, func() {
		closeCalls++
	})

	if closeCalls != 0 {
		t.Fatalf("transferred rename handle was unexpectedly closed %d times", closeCalls)
	}
}
