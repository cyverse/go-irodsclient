package session

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
)

// permanentError returns an error that getPendingError reports indefinitely, so the test does
// not depend on ConnectionCreationTimeout.
func permanentError() error {
	return types.NewConnectionConfigError(nil)
}

// acquireOrTimeout reports whether f returned before the deadline. A blocked acquire means the
// session mutex was not released.
func acquireOrTimeout(t *testing.T, f func()) bool {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// A pending connection error must not leave the session mutex held: the acquire after it used to
// block forever, wedging every later operation on the session.
func TestAcquireConnectionReleasesMutexOnPendingError(t *testing.T) {
	sess := &IRODSSession{
		lastConnectionError:     permanentError(),
		lastConnectionErrorTime: time.Now(),
	}

	if _, err := sess.AcquireConnection(true); err == nil {
		t.Fatal("AcquireConnection: expected the pending error to be returned")
	}

	if !acquireOrTimeout(t, func() { _, _ = sess.AcquireConnection(true) }) {
		t.Fatal("AcquireConnection deadlocked: the mutex was not released on the pending-error path")
	}
}
