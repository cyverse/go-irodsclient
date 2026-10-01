package fs

import (
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
)

// parallelUploadReadBufferDepth is the number of read buffers each upload task keeps.
// A depth of 2 (double buffering) lets a task read the next block from the local disk while
// the previous block is still being sent to the server.
const parallelUploadReadBufferDepth int = 2

// parallelUploadBlock is a block read from the local file, waiting to be sent to the server
type parallelUploadBlock struct {
	buffer []byte
	offset int64
	length int
}

// parallelUploadReader reads blocks from a local file in a background goroutine so that an
// upload task does not have to wait for the disk between two write requests. Blocks are
// delivered in order, starting at offset, as a data object handle is written sequentially.
//
// A task takes a block with Next, sends it, and gives it back with Release. The block must not
// be touched after Release, as the reader goroutine reuses its buffer.
type parallelUploadReader struct {
	file       *os.File
	localPath  string
	offset     int64
	length     int64 // negative reads until EOF
	depth      int
	bufferSize int

	freeChan  chan []byte
	blockChan chan *parallelUploadBlock
	stopChan  chan struct{}

	mutex  sync.Mutex
	err    error
	closed bool
}

// newParallelUploadReader creates a parallelUploadReader and starts its reader goroutine.
// It reads length bytes from offset, or, when length is negative, until the end of the file.
func newParallelUploadReader(file *os.File, localPath string, offset int64, length int64, depth int, bufferSize int) *parallelUploadReader {
	if depth < 1 {
		depth = 1
	}

	reader := &parallelUploadReader{
		file:       file,
		localPath:  localPath,
		offset:     offset,
		length:     length,
		depth:      depth,
		bufferSize: bufferSize,

		freeChan:  make(chan []byte, depth),
		blockChan: make(chan *parallelUploadBlock, depth),
		stopChan:  make(chan struct{}),
	}

	for i := 0; i < depth; i++ {
		reader.freeChan <- make([]byte, bufferSize)
	}

	go reader.run()

	return reader
}

func (reader *parallelUploadReader) run() {
	// closing tells the task that the whole range has been read
	defer close(reader.blockChan)

	currentOffset := reader.offset
	remain := reader.length

	for reader.length < 0 || remain > 0 {
		bufferLen := reader.bufferSize
		if reader.length >= 0 && remain < int64(bufferLen) {
			bufferLen = int(remain)
		}

		var buffer []byte
		select {
		case buffer = <-reader.freeChan:
		case <-reader.stopChan:
			return
		}

		bytesRead, err := reader.file.ReadAt(buffer[:bufferLen], currentOffset)
		if bytesRead > 0 {
			block := &parallelUploadBlock{
				buffer: buffer,
				offset: currentOffset,
				length: bytesRead,
			}

			select {
			case reader.blockChan <- block:
			case <-reader.stopChan:
				return
			}

			currentOffset += int64(bytesRead)
			remain -= int64(bytesRead)
		} else {
			reader.freeChan <- buffer
		}

		if err != nil {
			if err != io.EOF {
				reader.setError(errors.Wrapf(err, "failed to read file %q", reader.localPath))
			}

			return
		}
	}
}

func (reader *parallelUploadReader) getError() error {
	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	return reader.err
}

func (reader *parallelUploadReader) setError(err error) {
	reader.mutex.Lock()
	defer reader.mutex.Unlock()

	if reader.err == nil {
		reader.err = err
	}
}

// Next returns the next block in order, or a nil block once the whole range has been read.
// Blocks that were read before a read failure are delivered first, so the error only shows up
// after the last readable block.
func (reader *parallelUploadReader) Next() (*parallelUploadBlock, error) {
	block, ok := <-reader.blockChan
	if !ok {
		return nil, reader.getError()
	}

	return block, nil
}

// Release gives a block back so that the reader goroutine can reuse its buffer
func (reader *parallelUploadReader) Release(block *parallelUploadBlock) {
	// every buffer is either held by the task or owned by the reader, so this never blocks
	reader.freeChan <- block.buffer
}

// Close stops the reader goroutine and reports the first read error
func (reader *parallelUploadReader) Close() error {
	reader.mutex.Lock()
	if reader.closed {
		reader.mutex.Unlock()
		return reader.err
	}
	reader.closed = true
	reader.mutex.Unlock()

	close(reader.stopChan)

	// drain so that the reader goroutine can exit
	for range reader.blockChan { //nolint
	}

	return reader.getError()
}
