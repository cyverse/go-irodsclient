package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestParallelDownloadWriterOrderAndContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const blockSize = 4096
	const numBlocks = 256

	expected := make([]byte, blockSize*numBlocks)
	for i := range expected {
		expected[i] = byte(i * 7)
	}

	var mutex sync.Mutex
	written := []int64{}

	w := newParallelDownloadWriter(f, path, parallelDownloadWriteBufferDepth, blockSize, func(offset int64, length int) {
		mutex.Lock()
		written = append(written, offset)
		mutex.Unlock()
	})

	for i := 0; i < numBlocks; i++ {
		buf, err := w.GetBuffer()
		if err != nil {
			t.Fatal(err)
		}
		copy(buf, expected[i*blockSize:(i+1)*blockSize])
		if err := w.Write(buf, int64(i*blockSize), blockSize); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for i, off := range written {
		if off != int64(i*blockSize) {
			t.Fatalf("out of order callback at %d: got %d", i, off)
		}
	}
	if len(written) != numBlocks {
		t.Fatalf("expected %d callbacks, got %d", numBlocks, len(written))
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatal("file content mismatch")
	}
}

func TestParallelDownloadWriterPropagatesWriteError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// reopen read-only so WriteAt fails
	f.Close()
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := newParallelDownloadWriter(f, path, parallelDownloadWriteBufferDepth, 16, nil)

	sawErr := false
	for i := 0; i < 100; i++ {
		buf, err := w.GetBuffer()
		if err != nil {
			sawErr = true
			break
		}
		if err := w.Write(buf, int64(i*16), 16); err != nil {
			sawErr = true
			break
		}
	}

	if !sawErr && w.Flush() == nil {
		t.Fatal("expected a write error to surface")
	}
	if w.Close() == nil {
		t.Fatal("expected Close to report the write error")
	}
}

func TestParallelDownloadWriterReleaseBuffer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := newParallelDownloadWriter(f, path, parallelDownloadWriteBufferDepth, 16, nil)

	// take and give back more buffers than the depth, must not deadlock
	for i := 0; i < 10; i++ {
		buf, err := w.GetBuffer()
		if err != nil {
			t.Fatal(err)
		}
		w.ReleaseBuffer(buf)
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
