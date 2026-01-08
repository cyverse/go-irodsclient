//go:build linux

package system

import (
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func getNetworkConfig() (*NetConfig, error) {
	config := &NetConfig{}

	// read net.core.wmem_max
	data, err := os.ReadFile("/proc/sys/net/core/wmem_max")
	if err != nil {
		return nil, errors.Wrap(err, "failed to read /proc/sys/net/core/wmem_max")
	}
	config.CoreWmemMax, err = strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse /proc/sys/net/core/wmem_max")
	}

	// read net.ipv4.tcp_wmem (Format: "min default max")
	data, err = os.ReadFile("/proc/sys/net/ipv4/tcp_wmem")
	if err != nil {
		return nil, errors.Wrap(err, "failed to read /proc/sys/net/ipv4/tcp_wmem")
	}

	parts := strings.Fields(string(data))
	if len(parts) >= 3 {
		maxVal, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse max value from /proc/sys/net/ipv4/tcp_wmem")
		}
		config.TcpWmemMax = maxVal
	} else {
		return nil, errors.New("invalid tcp_wmem format")
	}

	return config, nil
}
