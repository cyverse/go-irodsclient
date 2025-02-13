package util

import (
	"sync/atomic"
	"time"
)

type TimeoutWaitGroup struct {
	count int32
	done  chan struct{}
}

func NewTimeoutWaitGroup() *TimeoutWaitGroup {
	return &TimeoutWaitGroup{
		done: make(chan struct{}),
	}
}

func (wg *TimeoutWaitGroup) Add(i int32) {
	select {
	case <-wg.done:
		panic("use of an already closed TimeoutWaitGroup")
	default:
	}

	atomic.AddInt32(&wg.count, i)
}

func (wg *TimeoutWaitGroup) Done() {
	i := atomic.AddInt32(&wg.count, -1)
	if i == 0 {
		close(wg.done)
	}
	if i < 0 {
		panic("too many Done() calls")
	}
}

func (wg *TimeoutWaitGroup) C() <-chan struct{} {
	return wg.done
}

func (wg *TimeoutWaitGroup) WaitTimeout(timeout time.Duration) bool {
	select {
	case <-wg.done:
		return true // done
	case <-time.After(timeout):
		return false // timed out
	}
}
