package types

// IRODSVersion contains irods version information
type IRODSVersion struct {
	Status         int
	ReleaseVersion string
	APIVersion     string
	ReconnectPort  int
	ReconnectAddr  string
	Cookie         int
}
