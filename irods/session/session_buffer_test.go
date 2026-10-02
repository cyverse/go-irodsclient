package session

import "testing"

func TestResolveRedirectionBufferSize(t *testing.T) {
	tests := []struct {
		name             string
		configuredSize   int
		serverWindowSize int
		poolSize         int
		want             int
	}{
		{
			// nothing asked for anything, the kernel sizes the buffer
			name: "all unset",
			want: 0,
		},
		{
			// the server says nothing, so the pool's size stands
			name:     "pool size only",
			poolSize: 4 * 1024 * 1024,
			want:     4 * 1024 * 1024,
		},
		{
			// an administrator asked for a window size with msiSetNumThreads
			name:             "server window size",
			serverWindowSize: 1024 * 1024,
			poolSize:         4 * 1024 * 1024,
			want:             1024 * 1024,
		},
		{
			// the caller configured a size, which wins over everything else
			name:             "configured size wins",
			configuredSize:   2 * 1024 * 1024,
			serverWindowSize: 1024 * 1024,
			poolSize:         4 * 1024 * 1024,
			want:             2 * 1024 * 1024,
		},
		{
			// a server that sends nonsense must not shrink or disable anything
			name:             "negative server window size",
			serverWindowSize: -1,
			poolSize:         4 * 1024 * 1024,
			want:             4 * 1024 * 1024,
		},
	}

	for _, test := range tests {
		size := resolveRedirectionBufferSize(test.configuredSize, test.serverWindowSize, test.poolSize)

		if size != test.want {
			t.Errorf("%s: got %d, want %d", test.name, size, test.want)
		}
	}
}
