package session

import (
	"container/list"
	"sync"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
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

// Waiting for a full pool must not hold the session mutex. ReturnConnection needs that mutex
// before it can return a connection to the pool and wake the waiter.
func TestSupportParallelUploadReleasesMutexWhileWaitingForConnection(t *testing.T) {
	occupiedConn := &connection.IRODSConnection{}
	pool := &ConnectionPool{
		config:              &ConnectionPoolConfig{MaxCap: 1},
		logger:              log.New().WithField("test", t.Name()),
		idleConnections:     list.New(),
		occupiedConnections: map[*connection.IRODSConnection]bool{occupiedConn: true},
		callbacks:           map[string]ConnectionUsageCallback{},
		mutex:               sync.Mutex{},
		terminateChan:       make(chan bool),
	}
	pool.waitCond = sync.NewCond(&pool.mutex)

	sess := &IRODSSession{
		config:            &IRODSSessionConfig{},
		connectionPool:    pool,
		sharedConnections: map[*connection.IRODSConnection]int{occupiedConn: 1},
		logger:            log.New().WithField("test", t.Name()),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sess.SupportParallelUpload()
	}()

	if !acquireOrTimeout(t, func() {
		sess.mutex.Lock()
		sess.mutex.Unlock()
	}) {
		t.Fatal("SupportParallelUpload held the session mutex while waiting for the pool")
	}

	// Stop the synthetic pool and wake SupportParallelUpload so the test leaves no goroutine.
	pool.mutex.Lock()
	pool.terminated = true
	pool.waitCond.Broadcast()
	pool.mutex.Unlock()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SupportParallelUpload did not stop after the pool was terminated")
	}
}
