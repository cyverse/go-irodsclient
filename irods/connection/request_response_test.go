package connection

import (
	"errors"
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/message"
)

type failingAsyncRequest struct{}

func (failingAsyncRequest) GetMessage() (*message.IRODSMessage, error) {
	return nil, errors.New("request construction failed")
}

func (failingAsyncRequest) GetXMLCorrector() message.XMLCorrector {
	return nil
}

// Submitting requests must not depend on concurrently draining the output channel. This used to
// deadlock after the fixed-size wait and output buffers both filled.
func TestRequestAsyncDoesNotBlockInputWhenOutputIsNotDrained(t *testing.T) {
	conn := &IRODSConnection{config: &IRODSConnectionConfig{}}
	input := make(chan RequestResponsePair)
	output := conn.RequestAsyncWithTrackerCallBack(input)

	const requestCount = 1000
	submitted := make(chan struct{})
	go func() {
		defer close(submitted)
		for i := 0; i < requestCount; i++ {
			input <- RequestResponsePair{Request: failingAsyncRequest{}}
		}
		close(input)
	}()

	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("request submission blocked because output was not being drained")
	}

	resultCount := 0
	for pair := range output {
		if pair.Error == nil {
			t.Fatal("expected every request after the construction failure to contain an error")
		}
		resultCount++
	}

	if resultCount != requestCount {
		t.Fatalf("received %d results, want %d", resultCount, requestCount)
	}
}
