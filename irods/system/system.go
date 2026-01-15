package system

import "github.com/cockroachdb/errors"

const (
	// user data chunk size for network transfer (4MB)
	chunkSize = 4 * 1024 * 1024

	// Kernel overhead factor
	// we need to multiply this factor to chunkSize to cover kernel overhead
	KernelOverheadFactor = 2

	// TargetKernelBufferSize is the target kernel buffer size for optimal performance
	TargetKernelBufferSize = chunkSize * KernelOverheadFactor
)

type NetConfig struct {
	CoreWmemMax int
	TcpWmemMax  int
}

func GetNetworkConfig() (*NetConfig, error) {
	return getNetworkConfig()
}

func GetTCPBufferSize() (int, bool, error) {
	netConfig, err := getNetworkConfig()
	if err != nil {
		return 0, false, errors.Wrapf(err, "failed to get system suggested buffer size")
	}

	if netConfig.CoreWmemMax <= 0 && netConfig.TcpWmemMax <= 0 {
		// use system default
		return 0, false, nil
	}

	if netConfig.TcpWmemMax > 0 && netConfig.TcpWmemMax > netConfig.CoreWmemMax {
		// use tcp wmem max by not setting tcp buffer size
		return netConfig.TcpWmemMax / KernelOverheadFactor, false, nil
	}

	if netConfig.CoreWmemMax > 0 && netConfig.CoreWmemMax >= netConfig.TcpWmemMax {
		return netConfig.CoreWmemMax / KernelOverheadFactor, true, nil
	}

	return 0, false, nil
}
