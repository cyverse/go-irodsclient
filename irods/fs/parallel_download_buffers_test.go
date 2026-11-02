package fs

import "testing"

func TestAdjustNumBuffersForParallelDownloadDefaultsToTwoPerTask(t *testing.T) {
	tests := []struct {
		numBuffers int
		numTasks   int
		want       int
	}{
		{numBuffers: 0, numTasks: 4, want: 8},
		{numBuffers: -1, numTasks: 1, want: 2},
		{numBuffers: 3, numTasks: 4, want: 4},
		{numBuffers: 4, numTasks: 4, want: 4},
		{numBuffers: 16, numTasks: 4, want: 16},
	}

	for _, tt := range tests {
		if got := adjustNumBuffersForParallelDownload(tt.numBuffers, tt.numTasks); got != tt.want {
			t.Fatalf("adjustNumBuffersForParallelDownload(%d, %d) = %d, want %d", tt.numBuffers, tt.numTasks, got, tt.want)
		}
	}
}
