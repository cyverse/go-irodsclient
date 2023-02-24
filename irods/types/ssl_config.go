package types

import (
	"os"

	"golang.org/x/xerrors"
)

// IRODSSSLConfig contains irods ssl configuration
type IRODSSSLConfig struct {
	CACertificateFile   string
	EncryptionKeySize   int
	EncryptionAlgorithm string
	SaltSize            int
	HashRounds          int
}

// CreateIRODSSSLConfig creates IRODSSSLConfig
func CreateIRODSSSLConfig(caCertFile string, keySize int, algorithm string, saltSize int,
	hashRounds int) (*IRODSSSLConfig, error) {
	return &IRODSSSLConfig{
		CACertificateFile:   caCertFile,
		EncryptionKeySize:   keySize,
		EncryptionAlgorithm: algorithm,
		SaltSize:            saltSize,
		HashRounds:          hashRounds,
	}, nil
}

// ReadCACert returns CA Cert data
func (config *IRODSSSLConfig) ReadCACert() ([]byte, error) {
	if len(config.CACertificateFile) > 0 {
		caCert, err := os.ReadFile(config.CACertificateFile)
		if err != nil {
			return nil, xerrors.Errorf("failed to read from file %s: %w", config.CACertificateFile, err)
		}
		return caCert, nil
	}

	return nil, xerrors.Errorf("ca certificate file is not set")
}
