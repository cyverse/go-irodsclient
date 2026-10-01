//go:build linux

package system

import (
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// readSysctlInt reads a single integer from a /proc/sys file
func readSysctlInt(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read %s", path)
	}

	value, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse %s", path)
	}

	return value, nil
}

// readSysctlMax reads the last of the "min default max" triples in a /proc/sys file
func readSysctlMax(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read %s", path)
	}

	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, errors.Errorf("invalid format in %s, expected three fields, got %d", path, len(fields))
	}

	value, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse max value from %s", path)
	}

	return value, nil
}

func getNetworkConfig() (*NetConfig, error) {
	config := &NetConfig{}
	var err error

	config.CoreWmemMax, err = readSysctlInt("/proc/sys/net/core/wmem_max")
	if err != nil {
		return nil, err
	}

	config.CoreRmemMax, err = readSysctlInt("/proc/sys/net/core/rmem_max")
	if err != nil {
		return nil, err
	}

	config.TcpWmemMax, err = readSysctlMax("/proc/sys/net/ipv4/tcp_wmem")
	if err != nil {
		return nil, err
	}

	config.TcpRmemMax, err = readSysctlMax("/proc/sys/net/ipv4/tcp_rmem")
	if err != nil {
		return nil, err
	}

	return config, nil
}
