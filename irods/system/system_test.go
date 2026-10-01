package system

import "testing"

func TestTcpBufferSizeForLeavesTunedSystemsAlone(t *testing.T) {
	tests := []struct {
		name    string
		coreMax int
		tcpMax  int
	}{
		{
			// a stock linux box: the kernel grows the buffer far past what setsockopt may ask
			// for, so pinning it would only make things worse
			name:    "stock linux",
			coreMax: 212992,
			tcpMax:  4 * 1024 * 1024,
		},
		{
			// the receive side of the machine this was written on
			name:    "large tcp_rmem",
			coreMax: 4 * 1024 * 1024,
			tcpMax:  32 * 1024 * 1024,
		},
		{
			// macOS reports one ceiling for everything, at or above the target
			name:    "macos default",
			coreMax: 8 * 1024 * 1024,
			tcpMax:  8 * 1024 * 1024,
		},
		{
			name:    "unknown limits",
			coreMax: 0,
			tcpMax:  0,
		},
		{
			name:    "core limit unknown",
			coreMax: 0,
			tcpMax:  1024,
		},
	}

	for _, test := range tests {
		if size := tcpBufferSizeFor(test.coreMax, test.tcpMax); size != 0 {
			t.Errorf("%s: asked for %d, want the socket left alone", test.name, size)
		}
	}
}

func TestTcpBufferSizeForRaisesWhatTheKernelCannotReach(t *testing.T) {
	tests := []struct {
		name    string
		coreMax int
		tcpMax  int
		want    int
	}{
		{
			// the send side of the machine this was written on: the kernel stops at 4MB, and
			// setsockopt reaches 8MB because the kernel doubles the request
			name:    "send side, core allows the target",
			coreMax: 4 * 1024 * 1024,
			tcpMax:  4 * 1024 * 1024,
			want:    4 * 1024 * 1024,
		},
		{
			// the core limit is the binding one, ask for all of it
			name:    "core limit below the target",
			coreMax: 3 * 1024 * 1024,
			tcpMax:  1024 * 1024,
			want:    3 * 1024 * 1024,
		},
		{
			// never ask for more than the target, even when the system would allow it
			name:    "core limit far above the target",
			coreMax: 64 * 1024 * 1024,
			tcpMax:  1024 * 1024,
			want:    TargetKernelBufferSize / KernelOverheadFactor,
		},
	}

	for _, test := range tests {
		size := tcpBufferSizeFor(test.coreMax, test.tcpMax)
		if size != test.want {
			t.Errorf("%s: asked for %d, want %d", test.name, size, test.want)
		}

		// the point of asking at all is to beat what the kernel reaches on its own
		if size*KernelOverheadFactor <= test.tcpMax {
			t.Errorf("%s: a request of %d yields %d, which does not beat the kernel's %d",
				test.name, size, size*KernelOverheadFactor, test.tcpMax)
		}

		if size > test.coreMax {
			t.Errorf("%s: asked for %d, above the core limit %d", test.name, size, test.coreMax)
		}
	}
}

// TestTcpBufferSizeForNeverAsksForLessThanTheKernel guards the direction of the overhead
// factor: the kernel doubles a request, so dividing the limit by it again would ask for half
// of what the system allows
func TestTcpBufferSizeForNeverAsksForLessThanTheKernel(t *testing.T) {
	for coreMax := 64 * 1024; coreMax <= 64*1024*1024; coreMax *= 2 {
		for tcpMax := 64 * 1024; tcpMax <= 64*1024*1024; tcpMax *= 2 {
			size := tcpBufferSizeFor(coreMax, tcpMax)
			if size == 0 {
				continue
			}

			if size*KernelOverheadFactor <= tcpMax {
				t.Fatalf("coreMax %d, tcpMax %d: asked for %d, which yields %d and loses to the kernel",
					coreMax, tcpMax, size, size*KernelOverheadFactor)
			}
		}
	}
}

func TestGetTCPBufferSizesStaysWithinTheSystemLimits(t *testing.T) {
	netConfig, err := GetNetworkConfig()
	if err != nil {
		t.Skipf("cannot read the system's network config: %v", err)
	}

	sendSize, recvSize, err := GetTCPBufferSizes()
	if err != nil {
		t.Fatal(err)
	}

	if sendSize < 0 || recvSize < 0 {
		t.Fatalf("negative buffer sizes, send %d, receive %d", sendSize, recvSize)
	}

	if sendSize > netConfig.CoreWmemMax {
		t.Fatalf("send size %d is above net.core.wmem_max %d", sendSize, netConfig.CoreWmemMax)
	}

	if recvSize > netConfig.CoreRmemMax {
		t.Fatalf("receive size %d is above net.core.rmem_max %d", recvSize, netConfig.CoreRmemMax)
	}

	t.Logf("wmem_max %d, tcp_wmem max %d -> send %d", netConfig.CoreWmemMax, netConfig.TcpWmemMax, sendSize)
	t.Logf("rmem_max %d, tcp_rmem max %d -> receive %d", netConfig.CoreRmemMax, netConfig.TcpRmemMax, recvSize)
}
