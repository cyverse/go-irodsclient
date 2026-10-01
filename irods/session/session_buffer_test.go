package session

import "testing"

func TestResolveRedirectionBufferSizes(t *testing.T) {
	tests := []struct {
		name             string
		configuredSize   int
		serverWindowSize int
		poolSendSize     int
		poolRecvSize     int
		wantSend         int
		wantRecv         int
	}{
		{
			// nothing asked for anything, the kernel sizes the buffers
			name:     "all unset",
			wantSend: 0,
			wantRecv: 0,
		},
		{
			// the server says nothing, so the pool's per direction sizes stand
			name:         "pool sizes only",
			poolSendSize: 4 * 1024 * 1024,
			poolRecvSize: 0,
			wantSend:     4 * 1024 * 1024,
			wantRecv:     0,
		},
		{
			// an administrator asked for a window size with msiSetNumThreads
			name:             "server window size",
			serverWindowSize: 1024 * 1024,
			poolSendSize:     4 * 1024 * 1024,
			poolRecvSize:     0,
			wantSend:         1024 * 1024,
			wantRecv:         1024 * 1024,
		},
		{
			// the caller configured a size, which wins over everything else
			name:             "configured size wins",
			configuredSize:   2 * 1024 * 1024,
			serverWindowSize: 1024 * 1024,
			poolSendSize:     4 * 1024 * 1024,
			poolRecvSize:     8 * 1024 * 1024,
			wantSend:         2 * 1024 * 1024,
			wantRecv:         2 * 1024 * 1024,
		},
		{
			// a server that sends nonsense must not shrink or disable anything
			name:             "negative server window size",
			serverWindowSize: -1,
			poolSendSize:     4 * 1024 * 1024,
			poolRecvSize:     0,
			wantSend:         4 * 1024 * 1024,
			wantRecv:         0,
		},
	}

	for _, test := range tests {
		sendSize, recvSize := resolveRedirectionBufferSizes(test.configuredSize, test.serverWindowSize, test.poolSendSize, test.poolRecvSize)

		if sendSize != test.wantSend || recvSize != test.wantRecv {
			t.Errorf("%s: got send %d receive %d, want send %d receive %d",
				test.name, sendSize, recvSize, test.wantSend, test.wantRecv)
		}
	}
}
