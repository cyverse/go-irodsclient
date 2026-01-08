//go:build darwin

package system

import "golang.org/x/sys/unix"

func getNetworkConfig() (*NetConfig, error) {
	// macOS uses kern.ipc.maxsockbuf for socket buffer maximum size.
	val, err := unix.SysctlUint32("kern.ipc.maxsockbuf")
	if err != nil {
		return nil, err
	}

	return &NetConfig{
		CoreWmemMax: int(val),
		TcpWmemMax:  int(val), // macOS treats this value as the TCP max limit
	}, nil
}
