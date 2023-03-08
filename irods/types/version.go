package types

import (
	"strconv"
	"strings"
)

// IRODSVersion contains irods version information
type IRODSVersion struct {
	ReleaseVersion string // e.g., "rods4.2.8"
	APIVersion     string
	ReconnectPort  int
	ReconnectAddr  string
	Cookie         int
}

// GetReleaseVersion returns version parts (major, minor, patch)
func (ver *IRODSVersion) GetReleaseVersion() (int, int, int) {
	major := 0
	minor := 0
	patch := 0

	releaseVersion := strings.ToLower(ver.ReleaseVersion)
	releaseVersion = strings.TrimPrefix(releaseVersion, "rods")

	vers := strings.Split(releaseVersion, ".")
	if len(vers) >= 1 {
		m, err := strconv.Atoi(vers[0])
		if err == nil {
			major = m
		}
	}

	if len(vers) >= 2 {
		m, err := strconv.Atoi(vers[1])
		if err == nil {
			minor = m
		}
	}

	if len(vers) >= 3 {
		p, err := strconv.Atoi(vers[2])
		if err == nil {
			patch = p
		}
	}

	return major, minor, patch
}

// HasHigherVersionThan returns if given version is higher or equal than current version
func (ver *IRODSVersion) HasHigherVersionThan(major int, minor int, patch int) bool {
	smajor, sminor, spatch := ver.GetReleaseVersion()
	if smajor > major {
		return true
	}
	if smajor < major {
		return false
	}
	// major is equal
	if sminor > minor {
		return true
	}
	if sminor < minor {
		return false
	}
	// minor is equal
	if spatch > patch {
		return true
	}
	if spatch < patch {
		return false
	}
	return true
}
