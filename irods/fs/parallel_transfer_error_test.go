package fs

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReportParallelTransferErrorNeverBlocksWorkers(t *testing.T) {
	errChan := make(chan error, 1)
	var wait sync.WaitGroup

	const workers = 1000
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reportParallelTransferError(errChan, errors.New("transfer failed"))
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		wait.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("parallel transfer workers blocked while reporting errors")
	}

	if len(errChan) != 1 {
		t.Fatalf("recorded %d errors, want only the first error", len(errChan))
	}
}
