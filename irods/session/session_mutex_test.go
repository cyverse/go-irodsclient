package session

import (
	"container/list"
	"errors"
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

func TestAcquireConnectionsMultiReleasesMutexWhileConnecting(t *testing.T) {
	account, err := types.CreateIRODSAccount("localhost", 1247, "user", "zone", types.AuthSchemeNative, "password", "")
	if err != nil {
		t.Fatalf("failed to create test account: %v", err)
	}
	pool, err := NewConnectionPool(account, &ConnectionPoolConfig{InitialCap: 0, MaxCap: 2})
	if err != nil {
		t.Fatalf("failed to create test pool: %v", err)
	}
	defer pool.Release()

	connectStarted := make(chan struct{}, 2)
	allowConnect := make(chan struct{})
	sess := &IRODSSession{
		config:            &IRODSSessionConfig{},
		connectionPool:    pool,
		sharedConnections: map[*connection.IRODSConnection]int{},
		logger:            log.New().WithField("test", t.Name()),
		connectionConnector: func(*connection.IRODSConnection) error {
			connectStarted <- struct{}{}
			<-allowConnect
			return errors.New("synthetic connection failure")
		},
	}

	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		_, _ = sess.AcquireConnectionsMulti(2, false)
	}()
	<-connectStarted
	<-connectStarted

	if !acquireOrTimeout(t, func() {
		sess.mutex.Lock()
		sess.mutex.Unlock()
	}) {
		t.Fatal("AcquireConnectionsMulti held the session mutex during Connect")
	}

	close(allowConnect)
	select {
	case <-acquireDone:
	case <-time.After(5 * time.Second):
		t.Fatal("AcquireConnectionsMulti did not finish after connection attempts completed")
	}

	if len(sess.sharedConnections) != 0 {
		t.Fatalf("failed connections remain registered in session: %d", len(sess.sharedConnections))
	}
}

func TestReturnConnectionsMultiUsesSessionMutex(t *testing.T) {
	sess := &IRODSSession{
		sharedConnections: map[*connection.IRODSConnection]int{},
	}

	sess.mutex.Lock()
	returnDone := make(chan struct{})
	go func() {
		defer close(returnDone)
		_ = sess.ReturnConnectionsMulti(nil)
	}()

	select {
	case <-returnDone:
		sess.mutex.Unlock()
		t.Fatal("ReturnConnectionsMulti modified session state without acquiring the session mutex")
	case <-time.After(100 * time.Millisecond):
	}

	sess.mutex.Unlock()
	select {
	case <-returnDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ReturnConnectionsMulti did not finish after the session mutex was released")
	}
}
