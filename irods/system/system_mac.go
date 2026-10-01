//go:build darwin

package system

import "golang.org/x/sys/unix"

func getNetworkConfig() (*NetConfig, error) {
	// macOS uses kern.ipc.maxsockbuf for socket buffer maximum size.
	val, err := unix.SysctlUint32("kern.ipc.maxsockbuf")
	if err != nil {
		return nil, err
	}

	// macOS caps every socket buffer with this one value, and tunes the buffers below it
	return &NetConfig{
		CoreWmemMax: int(val),
		CoreRmemMax: int(val),
		TcpWmemMax:  int(val),
		TcpRmemMax:  int(val),
	}, nil
}
