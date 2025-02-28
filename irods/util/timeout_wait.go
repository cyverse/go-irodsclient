package util

import (
	"sync"
	"time"
)

type TimeoutWaitGroup struct {
	wg      sync.WaitGroup
	done    chan struct{}
	mutex   sync.Mutex
	counter int
}

func NewTimeoutWaitGroup() *TimeoutWaitGroup {
	return &TimeoutWaitGroup{
		done: make(chan struct{}),
	}
}

func (wg *TimeoutWaitGroup) Add(i int) {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	if wg.counter == 0 && i > 0 {
		wg.done = make(chan struct{})
	}

	wg.counter += i
	wg.wg.Add(i)

	if wg.counter == 0 {
		close(wg.done)
	}
}

func (wg *TimeoutWaitGroup) Done() {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	wg.counter--
	if wg.counter == 0 {
		close(wg.done)
	}

	wg.wg.Done()
}

func (wg *TimeoutWaitGroup) Wait() {
	wg.wg.Wait()
}

func (wg *TimeoutWaitGroup) WaitTimeout(timeout time.Duration) bool {
	wg.mutex.Lock()
	if wg.counter == 0 {
		wg.mutex.Unlock()
		return true
	}
	done := wg.done
	wg.mutex.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
