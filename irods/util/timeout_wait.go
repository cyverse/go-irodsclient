package util

import (
	"sync"
	"time"
)

// TimeoutWaitGroup is a WaitGroup that supports timeout on Wait operations.
// It wraps sync.WaitGroup and provides WaitFor with timeout support.
type TimeoutWaitGroup struct {
	wg       sync.WaitGroup
	mu       sync.Mutex
	counter  int
	done     chan struct{}
	signaled bool
}

func NewTimeoutWaitGroup() *TimeoutWaitGroup {
	done := make(chan struct{})
	close(done)
	return &TimeoutWaitGroup{done: done, signaled: true}
}

// Add adds delta, which may be negative, to the WaitGroup counter.
// If the counter becomes zero and there are queued goroutines, they will be released.
func (twg *TimeoutWaitGroup) Add(delta int) {
	twg.mu.Lock()
	defer twg.mu.Unlock()

	if twg.counter+delta < 0 {
		panic("sync: negative WaitGroup counter")
	}
	if twg.counter == 0 && delta > 0 && twg.signaled {
		twg.done = make(chan struct{})
		twg.signaled = false
	}

	twg.counter += delta
	if twg.counter == 0 && !twg.signaled {
		twg.signaled = true
		close(twg.done)
	}

	twg.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one.
func (twg *TimeoutWaitGroup) Done() {
	twg.mu.Lock()
	twg.counter--
	if twg.counter == 0 && !twg.signaled {
		twg.signaled = true
		close(twg.done)
	}
	twg.mu.Unlock()
	twg.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (twg *TimeoutWaitGroup) Wait() {
	twg.wg.Wait()
}

// WaitFor waits for the group to complete, or for the timeout duration to elapse.
// Returns true if all tasks completed, false if timeout occurred.
func (twg *TimeoutWaitGroup) WaitFor(timeout time.Duration) bool {
	twg.mu.Lock()
	done := twg.done
	twg.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
