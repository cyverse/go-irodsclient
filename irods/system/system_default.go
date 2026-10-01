//go:build !linux && !darwin

package system

func getNetworkConfig() (*NetConfig, error) {
	// unknown platform, leave every socket buffer to the operating system
	return &NetConfig{}, nil
}
