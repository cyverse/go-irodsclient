package fs

import (
	"errors"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
)

func TestDrainWriteDataObjectAsyncResponsesContinuesAfterError(t *testing.T) {
	responseChan := make(chan connection.RequestResponsePair)
	pipelineFailed := make(chan struct{})
	wantErr := errors.New("write failed")

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		responseChan <- connection.RequestResponsePair{Error: wantErr}
		for i := 0; i < 1000; i++ {
			responseChan <- connection.RequestResponsePair{}
		}
		close(responseChan)
	}()

	resultChan := make(chan error, 1)
	go func() {
		resultChan <- drainWriteDataObjectAsyncResponses(responseChan, "/file", pipelineFailed)
	}()

	select {
	case <-pipelineFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("response drain did not signal the first pipeline error")
	}

	select {
	case <-producerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("response drain stopped after the first error")
	}

	select {
	case gotErr := <-resultChan:
		if !errors.Is(gotErr, wantErr) {
			t.Fatalf("drain returned %v, want %v", gotErr, wantErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("response drain did not finish")
	}
}
