package types

import (
	"fmt"
	"os"
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
			return nil, err
		}
		return caCert, nil
	}

	return nil, fmt.Errorf("CACertificateFile is not set")
}
