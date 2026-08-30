package fs

import (
	"reflect"
	"testing"
)

func TestDirCacheDoesNotShareInputOrOutputSlices(t *testing.T) {
	config := NewDefaultCacheConfig()
	cache := NewFileSystemCache(&config, "dir-cache-slice-test")
	defer cache.Close()

	// Extra capacity makes append reuse the caller's backing array if AddDirCache stores it as-is.
	input := make([]string, 2, 4)
	input[0] = "/dir/a"
	input[1] = "/dir/b"
	cache.AddDirCache("/dir", input)

	input[0] = "/dir/changed"
	input = append(input, "/dir/c")

	want := []string{"/dir/a", "/dir/b"}
	first := cache.GetDirCache("/dir")
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("cached input changed through caller slice: got %v, want %v", first, want)
	}

	first[0] = "/dir/output-changed"
	first = append(first, "/dir/d")

	second := cache.GetDirCache("/dir")
	if !reflect.DeepEqual(second, want) {
		t.Fatalf("cache changed through returned slice: got %v, want %v", second, want)
	}
}

func TestDirCachePreservesCachedEmptyDirectory(t *testing.T) {
	config := NewDefaultCacheConfig()
	cache := NewFileSystemCache(&config, "empty-dir-cache-test")
	defer cache.Close()

	cache.AddDirCache("/empty", []string{})
	entries := cache.GetDirCache("/empty")
	if entries == nil {
		t.Fatal("cached empty directory was returned as a cache miss")
	}
	if len(entries) != 0 {
		t.Fatalf("cached empty directory contains %d entries", len(entries))
	}
}
