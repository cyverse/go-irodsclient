package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t *testing.T, size int) (string, []byte) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "in.bin")

	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i * 7)
	}

	if err := os.WriteFile(path, content, 0666); err != nil {
		t.Fatal(err)
	}

	return path, content
}

func drainParallelUploadReader(t *testing.T, reader *parallelUploadReader) []byte {
	t.Helper()

	got := []byte{}
	lastOffset := int64(-1)

	for {
		block, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}

		if block == nil {
			break
		}

		if block.offset <= lastOffset {
			t.Fatalf("blocks out of order: %d after %d", block.offset, lastOffset)
		}
		lastOffset = block.offset

		got = append(got, block.buffer[:block.length]...)
		reader.Release(block)
	}

	return got
}

func TestParallelUploadReaderReadsUntilEOF(t *testing.T) {
	const blockSize = 4096
	const size = blockSize*10 + 123

	path, content := writeTempFile(t, size)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reader := newParallelUploadReader(f, path, 0, -1, parallelUploadReadBufferDepth, blockSize)

	got := drainParallelUploadReader(t, reader)

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch, got %d bytes, want %d", len(got), len(content))
	}
}

func TestParallelUploadReaderReadsRange(t *testing.T) {
	const blockSize = 1024
	const size = blockSize * 20

	path, content := writeTempFile(t, size)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// a partition that does not start or end on a block boundary
	offset := int64(blockSize*3 + 7)
	length := int64(blockSize*5 + 11)

	reader := newParallelUploadReader(f, path, offset, length, parallelUploadReadBufferDepth, blockSize)

	got := drainParallelUploadReader(t, reader)

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, content[offset:offset+length]) {
		t.Fatalf("content mismatch, got %d bytes, want %d", len(got), length)
	}
}

func TestParallelUploadReaderCloseStopsEarly(t *testing.T) {
	const blockSize = 512
	const size = blockSize * 1000

	path, _ := writeTempFile(t, size)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reader := newParallelUploadReader(f, path, 0, size, parallelUploadReadBufferDepth, blockSize)

	// take a couple of blocks, then abandon the rest, as a failed write would
	for range 3 {
		block, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if block == nil {
			t.Fatal("unexpected end of file")
		}
		reader.Release(block)
	}

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	// closing twice must be safe
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParallelUploadReaderPropagatesReadError(t *testing.T) {
	const blockSize = 1024

	path, _ := writeTempFile(t, blockSize*4)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	// close the file so that every read fails
	f.Close()

	reader := newParallelUploadReader(f, path, 0, blockSize*4, parallelUploadReadBufferDepth, blockSize)

	block, readErr := reader.Next()
	if block != nil {
		t.Fatal("expected no block from a closed file")
	}
	if readErr == nil {
		t.Fatal("expected a read error to surface")
	}

	if reader.Close() == nil {
		t.Fatal("expected Close to report the read error")
	}
}
