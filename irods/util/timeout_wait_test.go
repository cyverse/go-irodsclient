package util

import (
	"testing"
	"time"
)

func TestTimeoutWaitGroupInitiallyComplete(t *testing.T) {
	twg := NewTimeoutWaitGroup()
	if !twg.WaitFor(time.Second) {
		t.Fatal("zero counter should already be complete")
	}
}

func TestTimeoutWaitGroupCanBeReused(t *testing.T) {
	twg := NewTimeoutWaitGroup()

	twg.Add(1)
	twg.Done()
	if !twg.WaitFor(time.Second) {
		t.Fatal("first generation did not complete")
	}

	twg.Add(1)
	if twg.WaitFor(10 * time.Millisecond) {
		t.Fatal("second generation completed before Done")
	}
	twg.Done()
	if !twg.WaitFor(time.Second) {
		t.Fatal("second generation did not complete")
	}
}

func TestTimeoutWaitGroupWaitForTimesOut(t *testing.T) {
	twg := NewTimeoutWaitGroup()
	twg.Add(1)
	defer twg.Done()

	if twg.WaitFor(10 * time.Millisecond) {
		t.Fatal("WaitFor completed while work remained")
	}
}

func TestTimeoutWaitGroupRejectsNegativeCounter(t *testing.T) {
	twg := NewTimeoutWaitGroup()

	defer func() {
		if recover() == nil {
			t.Fatal("negative counter did not panic")
		}
	}()
	twg.Add(-1)
}
