package fs

// reportParallelTransferError records the first transfer error without blocking a worker.
// Callers only return one error, so retaining later errors can only create backpressure and
// prevent the worker's WaitGroup.Done from running.
func reportParallelTransferError(errChan chan<- error, err error) {
	if err == nil {
		return
	}

	select {
	case errChan <- err:
	default:
	}
}
