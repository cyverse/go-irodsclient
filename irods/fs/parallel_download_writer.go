package fs

import (
	"os"
	"sync"

	"github.com/cockroachdb/errors"
)

// parallelDownloadWriteBufferDepth is the number of read buffers each download task keeps.
// A depth of 2 (double buffering) lets a task read the next block from the network while
// the previous block is still being written to the local disk.
const parallelDownloadWriteBufferDepth int = 2

// parallelDownloadBlock is a block read from a data object, waiting to be written to the local file
type parallelDownloadBlock struct {
	buffer []byte
	offset int64
	length int
}

// parallelDownloadWriter writes blocks to a local file in a background goroutine so that a
// download task does not have to wait for the disk before issuing the next read request.
// Blocks are written in the order they are handed over, so a block is on disk only once every
// block handed over before it is on disk as well.
//
// A task owns a fixed set of buffers. It takes one with GetBuffer, fills it, and hands it over
// with Write or gives it back with ReleaseBuffer. The buffer must not be touched after Write,
// as the writer goroutine owns it until it returns from a later GetBuffer.
type parallelDownloadWriter struct {
	file      *os.File
	localPath string
	depth     int

	freeChan  chan []byte
	blockChan chan *parallelDownloadBlock
	doneChan  chan struct{}

	// onWritten is called from the writer goroutine right after a block lands on the local file
	onWritten func(offset int64, length int)

	mutex  sync.Mutex
	err    error
	closed bool
}

// newParallelDownloadWriter creates a parallelDownloadWriter and starts its writer goroutine.
// onWritten may be nil. It is called from the writer goroutine, so it must be safe to call
// concurrently with the writers of other tasks.
func newParallelDownloadWriter(file *os.File, localPath string, depth int, bufferSize int, onWritten func(offset int64, length int)) *parallelDownloadWriter {
	if depth < 1 {
		depth = 1
	}

	writer := &parallelDownloadWriter{
		file:      file,
		localPath: localPath,
		depth:     depth,

		freeChan:  make(chan []byte, depth),
		blockChan: make(chan *parallelDownloadBlock, depth),
		doneChan:  make(chan struct{}),

		onWritten: onWritten,
	}

	for i := 0; i < depth; i++ {
		writer.freeChan <- make([]byte, bufferSize)
	}

	go writer.run()

	return writer
}

func (writer *parallelDownloadWriter) run() {
	defer close(writer.doneChan)

	for block := range writer.blockChan {
		// stop writing after the first failure, but keep draining so that no task can block
		if writer.getError() == nil {
			_, err := writer.file.WriteAt(block.buffer[:block.length], block.offset)
			if err != nil {
				writer.setError(errors.Wrapf(err, "failed to write to file %q at offset %d", writer.localPath, block.offset))
			} else if writer.onWritten != nil {
				writer.onWritten(block.offset, block.length)
			}
		}

		// every buffer is either held by the task or owned by the writer, so this never blocks
		writer.freeChan <- block.buffer
	}
}

func (writer *parallelDownloadWriter) getError() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	return writer.err
}

func (writer *parallelDownloadWriter) setError(err error) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()

	if writer.err == nil {
		writer.err = err
	}
}

// GetBuffer returns a buffer to read into, waiting until the writer goroutine releases one
func (writer *parallelDownloadWriter) GetBuffer() ([]byte, error) {
	if err := writer.getError(); err != nil {
		return nil, err
	}

	buffer := <-writer.freeChan

	if err := writer.getError(); err != nil {
		writer.freeChan <- buffer
		return nil, err
	}

	return buffer, nil
}

// ReleaseBuffer gives a buffer back without writing it
func (writer *parallelDownloadWriter) ReleaseBuffer(buffer []byte) {
	writer.freeChan <- buffer
}

// Write hands a block over to the writer goroutine
func (writer *parallelDownloadWriter) Write(buffer []byte, offset int64, length int) error {
	if err := writer.getError(); err != nil {
		writer.ReleaseBuffer(buffer)
		return err
	}

	writer.blockChan <- &parallelDownloadBlock{
		buffer: buffer,
		offset: offset,
		length: length,
	}

	return nil
}

// Flush waits until every block handed over so far is on disk, then reports the first write error.
// The caller must not be holding a buffer taken from GetBuffer.
func (writer *parallelDownloadWriter) Flush() error {
	// taking every buffer means no block is in flight
	buffers := make([][]byte, 0, writer.depth)
	for i := 0; i < writer.depth; i++ {
		buffers = append(buffers, <-writer.freeChan)
	}

	for _, buffer := range buffers {
		writer.freeChan <- buffer
	}

	return writer.getError()
}

// Close stops the writer goroutine and reports the first write error
func (writer *parallelDownloadWriter) Close() error {
	writer.mutex.Lock()
	if writer.closed {
		writer.mutex.Unlock()
		return writer.err
	}
	writer.closed = true
	writer.mutex.Unlock()

	close(writer.blockChan)
	<-writer.doneChan

	return writer.getError()
}
