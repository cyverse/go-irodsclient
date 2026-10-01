package system

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

const (
	// KernelOverheadFactor is the factor the kernel multiplies a requested socket buffer size
	// by, to make room for its own bookkeeping. See the SO_RCVBUF note in socket(7).
	KernelOverheadFactor = 2

	// TargetKernelBufferSize is the kernel socket buffer a transfer wants, large enough to
	// hold one data chunk plus the kernel's bookkeeping overhead
	TargetKernelBufferSize = common.ReadWriteBufferSize * KernelOverheadFactor
)

// NetConfig holds the system's socket buffer limits.
// The core maxima cap what setsockopt may request, the tcp maxima cap how far the kernel
// grows a buffer on its own.
type NetConfig struct {
	// CoreWmemMax is net.core.wmem_max
	CoreWmemMax int
	// CoreRmemMax is net.core.rmem_max
	CoreRmemMax int
	// TcpWmemMax is the maximum of net.ipv4.tcp_wmem
	TcpWmemMax int
	// TcpRmemMax is the maximum of net.ipv4.tcp_rmem
	TcpRmemMax int
}

func GetNetworkConfig() (*NetConfig, error) {
	return getNetworkConfig()
}

// GetTCPBufferSizes returns the sizes to request with setsockopt for the send and the receive
// buffer. A zero size means that direction is left to the kernel.
func GetTCPBufferSizes() (int, int, error) {
	netConfig, err := getNetworkConfig()
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to get system suggested buffer size")
	}

	sendSize := tcpBufferSizeFor(netConfig.CoreWmemMax, netConfig.TcpWmemMax)
	recvSize := tcpBufferSizeFor(netConfig.CoreRmemMax, netConfig.TcpRmemMax)

	return sendSize, recvSize, nil
}

// tcpBufferSizeFor returns the size to request with setsockopt for one direction, or zero to
// leave that direction alone.
//
// The kernel sizes socket buffers on its own, growing them as a path needs up to the tcp_wmem
// or tcp_rmem maximum. Calling setsockopt turns that off and pins the buffer for the life of
// the socket, so it is only worth doing where the kernel cannot reach the size a transfer
// wants on its own.
//
// setsockopt caps the requested size at the matching net.core maximum and then multiplies it
// by KernelOverheadFactor, so requesting coreMax yields a kernel buffer of twice that.
func tcpBufferSizeFor(coreMax int, tcpMax int) int {
	if coreMax <= 0 {
		// nothing to go on, leave the socket to the kernel
		return 0
	}

	if tcpMax >= TargetKernelBufferSize {
		// the kernel already grows the buffer to the size a transfer wants
		return 0
	}

	request := min(TargetKernelBufferSize/KernelOverheadFactor, coreMax)

	if request*KernelOverheadFactor <= tcpMax {
		// pinning the buffer would not beat letting the kernel grow it
		return 0
	}

	return request
}
