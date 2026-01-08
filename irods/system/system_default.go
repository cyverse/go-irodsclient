//go:build !linux && !darwin

package system

func getNetworkConfig() (*NetConfig, error) {
	return &NetConfig{
		CoreWmemMax: 0, // default values
		TcpWmemMax:  0, // default values
	}, nil
}
